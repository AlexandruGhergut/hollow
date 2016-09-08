package com.netflix.vms.transformer.override;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.aws.file.FileStore;
import com.netflix.hollow.read.engine.HollowReadStateEngine;
import com.netflix.hollow.util.SimultaneousExecutor;
import com.netflix.vms.transformer.common.TransformerContext;
import com.netflix.vms.transformer.common.io.TransformerLogTag;
import com.netflix.vms.transformer.util.OverrideVipNameUtil;

/**
 * Manager to support Pinned Titles
 *
 * @author dsu
 */
public class PinTitleManager {
    private String proxyURL;
    private String inputDataVip;
    private String outputDataVip;

    private final String localBlobStore;
    private final TransformerContext ctx;
    private final FileStore fileStore;

    private final SimultaneousExecutor mainExecutor = new SimultaneousExecutor();
    private Map<PinTitleJobSpec, PinTitleProcessorJob> completedJobs = new HashMap<PinTitleJobSpec, PinTitleProcessorJob>();
    private Map<PinTitleJobSpec, PinTitleProcessorJob> failedJobs = new HashMap<PinTitleJobSpec, PinTitleProcessorJob>();
    private Map<PinTitleJobSpec, PinTitleProcessorJob> activeJobs = new HashMap<PinTitleJobSpec, PinTitleProcessorJob>();

    public PinTitleManager(FileStore fileStore, TransformerContext ctx) {
        this.fileStore = fileStore;
        this.localBlobStore = null;
        this.ctx = ctx;
    }

    public PinTitleManager(String proxyURL, String inputDataVip, String outputDataVip, String localBlobStore, TransformerContext ctx) {
        this.fileStore = null;
        this.localBlobStore = localBlobStore;
        this.ctx = ctx;

        this.proxyURL = proxyURL;
        this.inputDataVip = inputDataVip;
        this.outputDataVip = outputDataVip;
    }

    public synchronized void prepareForNextCycle() {
        // cleanup completed jobs that are no longer needed
        completedJobs = cleanupJobs(completedJobs, activeJobs);
        failedJobs = cleanupJobs(failedJobs, activeJobs);
    }

    private static Map<PinTitleJobSpec, PinTitleProcessorJob> cleanupJobs(Map<PinTitleJobSpec, PinTitleProcessorJob> existingJobs, Map<PinTitleJobSpec, PinTitleProcessorJob> activeJobs) {
        Map<PinTitleJobSpec, PinTitleProcessorJob> neededJobs = new HashMap<PinTitleJobSpec, PinTitleProcessorJob>();
        for (Map.Entry<PinTitleJobSpec, PinTitleProcessorJob> entry : existingJobs.entrySet()) {
            PinTitleJobSpec spec = entry.getKey();
            if (activeJobs.containsKey(spec)) {
                neededJobs.put(spec, entry.getValue());
            }
        }
        return neededJobs;
    }

    /**
     * Submit Jobs to be processed asynchronously
     */
    public synchronized void submitJobsToProcessASync(Set<String> pinnedTitleSpecs) throws Exception {
        // Execute them in parallel
        try {
            activeJobs = processSpecs(pinnedTitleSpecs);
            for (PinTitleProcessorJob job : activeJobs.values()) {
                mainExecutor.execute(job);
            }
        } catch (Exception ex) {
            ctx.getLogger().error(TransformerLogTag.CyclePinnedTitles, "Failed to submit job spec={} ex={}", pinnedTitleSpecs, ex);
            throw ex;
        }
    }

    /**
     * Return the result of the complete job
     */
    public List<HollowReadStateEngine> getResults(boolean isWaitForAllJobs) throws InterruptedException, ExecutionException {
        if (isWaitForAllJobs) {
            mainExecutor.awaitSuccessfulCompletionOfCurrentTasks();
        }

        List<HollowReadStateEngine> resultList = processResults(true);
        ctx.getLogger().info(TransformerLogTag.CyclePinnedTitles, "Misc Stat completedJobs={} failedJobs={} currJobs={} results={} waitedForAllJobs={}", completedJobs.size(), failedJobs.size(), activeJobs.size(), resultList.size(), isWaitForAllJobs);
        return resultList;
    }

    private synchronized List<HollowReadStateEngine> processResults(boolean isPropagateFailure) throws ExecutionException {
        // Collect Results on sorted Order
        List<HollowReadStateEngine> resultList = new ArrayList<>();
        for (PinTitleJobSpec jobSpec : sortJobSpecs(activeJobs.keySet())) {
            PinTitleProcessorJob job = activeJobs.get(jobSpec);
            if (!job.getStatus().isCompleted()) continue;

            if (job.isCompletedSuccessfully()) {
                resultList.add(job.getResult());
            } else {
                if (isPropagateFailure) {
                    throw new ExecutionException("Fail to process: " + job, job.getFailure());
                }
            }
        }
        return resultList;
    }

    private static List<PinTitleJobSpec> sortJobSpecs(Collection<PinTitleJobSpec> specs) {
        List<PinTitleJobSpec> result = new ArrayList<>(specs);
        Collections.sort(result); // Sort based on JobSpec ordering where oldest blob version is the first
        return result;
    }

    @VisibleForTesting
    void reset() {
        activeJobs.clear();
        failedJobs.clear();
        completedJobs.clear();
    }

    @VisibleForTesting
    Map<PinTitleJobSpec, PinTitleProcessorJob> getActiveJobs() {
        return activeJobs;
    }

    @VisibleForTesting
    Map<PinTitleJobSpec, PinTitleProcessorJob> getFailedJobs() {
        return failedJobs;
    }

    @VisibleForTesting
    Map<PinTitleJobSpec, PinTitleProcessorJob> getCompletedJobs() {
        return completedJobs;
    }


    // Convert the spec Strings into ProcessorJob
    @VisibleForTesting
    Map<PinTitleJobSpec, PinTitleProcessorJob> processSpecs(Set<String> pinnedTitleSpecs) {
        Map<PinTitleJobSpec, PinTitleProcessorJob> currJobs = new HashMap<>();

        if (pinnedTitleSpecs != null) {
            Map<String, PinTitleJobSpec> newSpecsMap = new HashMap<>();

            for (String spec : pinnedTitleSpecs) {
                try {
                    PinTitleJobSpec jobSpec = createJobSpec(spec);
                    PinTitleProcessorJob job = getExistingJob(jobSpec); // reuse last cycle's job if the same - to avoid re-processing
                    if (job == null) {
                        // prior result not found track/merge new jobs
                        trackNewJob(jobSpec, newSpecsMap);
                    } else {
                        currJobs.put(jobSpec, job);
                    }
                } catch (Exception ex) {
                    ctx.getLogger().error(TransformerLogTag.CyclePinnedTitles, "Failed to process spec={} ex={}", spec, ex);
                }
            }

            // Process new specs
            for (PinTitleJobSpec jobSpec : newSpecsMap.values()) {
                PinTitleProcessorJob job = createNewProcessJob(jobSpec);
                currJobs.put(jobSpec, job);
            }
        }

        return currJobs;
    }

    @VisibleForTesting
    PinTitleJobSpec createJobSpec(String spec) throws Exception {
        String parts[] = spec.split(":");
        long version = Long.parseLong(parts[0].trim());
        int[] topNodes = parseTopNodes(parts[1]);
        boolean isInputBased = false;
        if (parts.length >= 3) {
            isInputBased = "in".equals(parts[2]);
        }

        PinTitleJobSpec jobSpec = new PinTitleJobSpec(isInputBased, version, topNodes);
        return jobSpec;
    }

    private static void trackNewJob(PinTitleJobSpec newJobSpec, Map<String, PinTitleJobSpec> newSpecsMap) {
        PinTitleJobSpec jobSpec = newSpecsMap.get(newJobSpec.getID());
        if (jobSpec == null) {
            // add new one
            newSpecsMap.put(newJobSpec.getID(), newJobSpec);
        } else {
            // merge
            newSpecsMap.put(jobSpec.getID(), jobSpec.merge(newJobSpec));
        }
    }


    private int[] parseTopNodes(String value) throws Exception {
        int[] topNodes = null;

        if (value != null) {
            String[] parts = value.split(",");
            topNodes = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                topNodes[i] = Integer.parseInt(parts[i].trim());
            }
        }

        if (topNodes == null || topNodes.length == 0) {
            throw new Exception("topNodes is empty");
        }

        return topNodes;
    }

    private PinTitleProcessorJob getExistingJob(PinTitleJobSpec spec) {
        PinTitleProcessorJob job = completedJobs.get(spec);
        if (job!=null) return job;

        job = failedJobs.get(spec);
        if (job != null) return job;

        job = activeJobs.get(spec);
        return job;
    }


    // Create Job
    @VisibleForTesting
    PinTitleProcessorJob createNewProcessJob(PinTitleJobSpec jobSpec) {
        PinTitleProcessor processor;
        if (jobSpec.isInputBased) {
            processor = createInputBasedProcessor();
        } else {
            processor = createOutputBasedProcessor();
        }

        return new PinTitleProcessorJob(processor, jobSpec, ctx, new CompleteJobCallback() {
            @Override
            public void completedJob(PinTitleJobSpec jobSpec, PinTitleProcessorJob job, boolean isSuccessfull) {
                if (isSuccessfull) {
                    completedJobs.put(jobSpec, job);
                    failedJobs.remove(jobSpec);
                } else {
                    failedJobs.put(jobSpec, job);
                }
            }

        });
    }

    // Create Input Based Processor
    @VisibleForTesting
    PinTitleProcessor createInputBasedProcessor() {
        String vip = inputDataVip != null ? inputDataVip : ctx.getConfig().getConverterVip();
        if (fileStore != null) {
            return new InputSlicePinTitleProcessor(vip, fileStore, localBlobStore, ctx);
        } else {
            return new InputSlicePinTitleProcessor(vip, proxyURL, localBlobStore, ctx);
        }
    }

    // Create Output Based Processor
    @VisibleForTesting
    PinTitleProcessor createOutputBasedProcessor() {
        String vip = outputDataVip != null ? outputDataVip : OverrideVipNameUtil.getPinTitleDataTransformerVip(ctx.getConfig());
        if (fileStore != null) {
            return new OutputSlicePinTitleProcessor(vip, fileStore, localBlobStore, ctx);
        } else {
            return new OutputSlicePinTitleProcessor(vip, proxyURL, localBlobStore, ctx);
        }
    }

    private interface CompleteJobCallback {
        void completedJob(PinTitleJobSpec jobSpec, PinTitleProcessorJob job, boolean isSuccessfull);
    }

    public enum JobStatus {
        PENDING(false), RUNNING(false), COMPLETED_SUCC(true), COMPLETED_FAIL(true);

        private boolean isCompleted;

        JobStatus(boolean isCompleted) {
            this.isCompleted = isCompleted;
        }

        public boolean isCompleted() {
            return this.isCompleted;
        }
    }

    /**
     * Processor Job
     */
    public static class PinTitleProcessorJob implements Runnable, Comparable<PinTitleProcessorJob> {
        private final PinTitleProcessor processor;
        private final PinTitleJobSpec jobSpec;
        private final TransformerContext ctx;
        private final CompleteJobCallback callback;
        private HollowReadStateEngine resultStateEngine;
        private Throwable failure;
        private int retryCount = 0;
        protected JobStatus status = JobStatus.PENDING;

        PinTitleProcessorJob(PinTitleProcessor processor, PinTitleJobSpec jobSpec, TransformerContext ctx, CompleteJobCallback callback) {
            this.processor = processor;
            this.jobSpec = jobSpec;
            this.ctx = ctx;
            this.callback = callback;
        }

        @Override
        public synchronized void run() {
            if (isCompletedSuccessfully()) return;

            try {
                if (status == JobStatus.COMPLETED_FAIL) {
                    ctx.getLogger().error(TransformerLogTag.CyclePinnedTitles, "Retring failed job={} retry={}", this.jobSpec, ++retryCount);
                    resultStateEngine = processor.process(jobSpec.version, jobSpec.topNodes);
                } else {
                    status = JobStatus.RUNNING;
                    resultStateEngine = processor.process(jobSpec.version, jobSpec.topNodes);
                }

                status = JobStatus.COMPLETED_SUCC;
            } catch (Throwable ex) {
                status = JobStatus.COMPLETED_FAIL;
                String topNodes = Arrays.toString(jobSpec.topNodes);
                ctx.getLogger().error(TransformerLogTag.CyclePinnedTitles, "Failed to process override topNodes={} for version={} and vip={}, ex={}", topNodes, jobSpec.version, processor.getVip(), ex);
                failure = new Exception("Failed to process topNodes=" + topNodes + " for version=" + jobSpec.version + " on vip=" + processor.getVip(), ex);
            } finally {
                callback.completedJob(jobSpec, this, isCompletedSuccessfully());
            }
        }

        public JobStatus getStatus() {
            return status;
        }

        public boolean isCompletedSuccessfully() {
            return status == JobStatus.COMPLETED_SUCC;
        }

        public HollowReadStateEngine getResult() {
            return resultStateEngine;
        }

        public Throwable getFailure() {
            return failure;
        }

        @Override
        public int compareTo(PinTitleProcessorJob o) {
            return this.jobSpec.compareTo(o.jobSpec);
        }

        @Override
        public String toString() {
            return "PinTitleProcessorJob " + jobSpec;
        }
    }
}