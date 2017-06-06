package com.netflix.vms.transformer.publish.workflow;

import com.netflix.hollow.api.producer.HollowProducer.Publisher;

import com.netflix.hollow.api.producer.HollowProducer.Announcer;
import com.netflix.aws.file.FileStore;
import com.netflix.config.NetflixConfiguration.RegionEnum;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.vms.transformer.common.TransformerContext;
import com.netflix.vms.transformer.common.publish.workflow.PublicationJob;
import com.netflix.vms.transformer.common.slice.DataSlicer;
import com.netflix.vms.transformer.publish.status.CycleStatusFuture;
import com.netflix.vms.transformer.publish.status.WorkflowCycleStatusFuture;
import com.netflix.vms.transformer.publish.workflow.job.AfterCanaryAnnounceJob;
import com.netflix.vms.transformer.publish.workflow.job.AnnounceJob;
import com.netflix.vms.transformer.publish.workflow.job.AutoPinbackJob;
import com.netflix.vms.transformer.publish.workflow.job.BeforeCanaryAnnounceJob;
import com.netflix.vms.transformer.publish.workflow.job.CanaryAnnounceJob;
import com.netflix.vms.transformer.publish.workflow.job.CanaryRollbackJob;
import com.netflix.vms.transformer.publish.workflow.job.CanaryValidationJob;
import com.netflix.vms.transformer.publish.workflow.job.CircuitBreakerJob;
import com.netflix.vms.transformer.publish.workflow.job.DelayJob;
import com.netflix.vms.transformer.publish.workflow.job.HollowBlobPublishJob;
import com.netflix.vms.transformer.publish.workflow.job.HollowBlobPublishJob.PublishType;
import com.netflix.vms.transformer.publish.workflow.job.PoisonStateMarkerJob;
import com.netflix.vms.transformer.publish.workflow.job.framework.PublicationJobScheduler;
import com.netflix.vms.transformer.publish.workflow.job.impl.DefaultHollowPublishJobCreator;
import com.netflix.vms.transformer.publish.workflow.job.impl.HermesBlobAnnouncer;
import com.netflix.vms.transformer.publish.workflow.job.impl.ValuableVideoHolder;
import com.netflix.vms.transformer.publish.workflow.playbackmonkey.PlaybackMonkeyTester;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import netflix.admin.videometadata.uploadstat.ServerUploadStatus;

public class HollowPublishWorkflowStager implements PublishWorkflowStager {

    /* dependencies */
    private final TransformerContext ctx;
    private final PublicationJobScheduler scheduler;
    private final HollowBlobFileNamer fileNamer;
    private PublishRegionProvider regionProvider;
    private final DefaultHollowPublishJobCreator jobCreator;
    private HollowBlobDataProvider circuitBreakerDataProvider;

    /* fields */
    private final String vip;
    private final Map<RegionEnum, AnnounceJob> priorAnnouncedJobs;

    public HollowPublishWorkflowStager(TransformerContext ctx, FileStore fileStore, Publisher publisher, Publisher nostreamsPublisher, Announcer announcer, HermesBlobAnnouncer hermesBlobAnnouncer, DataSlicer dataSlicer, Supplier<ServerUploadStatus> uploadStatus, String vip) {
        this(ctx, fileStore, publisher, nostreamsPublisher, announcer, hermesBlobAnnouncer, new HollowBlobDataProvider(ctx), dataSlicer, uploadStatus, vip);
    }

    private HollowPublishWorkflowStager(TransformerContext ctx, FileStore fileStore, Publisher publisher, Publisher nostreamsPublisher, Announcer announcer, HermesBlobAnnouncer hermesBlobAnnouncer, HollowBlobDataProvider circuitBreakerDataProvider, DataSlicer dataSlicer, Supplier<ServerUploadStatus> uploadStatus, String vip) {
        this(ctx, new DefaultHollowPublishJobCreator(ctx, fileStore, publisher, nostreamsPublisher, announcer, hermesBlobAnnouncer, circuitBreakerDataProvider, new PlaybackMonkeyTester(), new ValuableVideoHolder(circuitBreakerDataProvider), dataSlicer, uploadStatus, vip), vip);
        this.circuitBreakerDataProvider = circuitBreakerDataProvider;
    }

    public HollowPublishWorkflowStager(TransformerContext ctx, DefaultHollowPublishJobCreator jobCreator, String vip) {
        this.ctx = ctx;
        this.scheduler = new PublicationJobScheduler();
        this.fileNamer = new HollowBlobFileNamer(vip);
        this.vip = vip;
        this.regionProvider = new PublishRegionProvider(ctx.getLogger());
        this.priorAnnouncedJobs = new HashMap<RegionEnum, AnnounceJob>();
        this.jobCreator = jobCreator;

        exposePublicationHistory();
    }

    @Override
    public void notifyRestoredStateEngine(HollowReadStateEngine restoredState, HollowReadStateEngine nostreamsRestoredState) {
        if(circuitBreakerDataProvider != null)
            circuitBreakerDataProvider.notifyRestoredStateEngine(restoredState, nostreamsRestoredState);
    }
    
    @Override
    public HollowReadStateEngine getCurrentReadStateEngine() {
        if(circuitBreakerDataProvider != null)
            return circuitBreakerDataProvider.getStateEngine();
        throw new IllegalStateException("No HollowReadStateEngine is available");
    }
    
    @Override
    public CycleStatusFuture triggerPublish(long inputDataVersion, long previousVersion, long newVersion) {
        PublishWorkflowContext ctx = jobCreator.beginStagingNewCycle();

        // Add validation job
        final CircuitBreakerJob circuitBreakerJob = addCircuitBreakerJob(previousVersion, newVersion);

        // Add publish jobs
        final List<PublicationJob> publishJobs = addPublishJobs(inputDataVersion, previousVersion, newVersion);

        // / add canary announcement and validation jobs
        final CanaryValidationJob canaryValidationJob = addCanaryJobs(previousVersion, newVersion, circuitBreakerJob, publishJobs);

        final AnnounceJob primaryRegionAnnounceJob = createAnnounceJobForRegion(regionProvider.getPrimaryRegion(), previousVersion, newVersion, canaryValidationJob, null);

        // / add secondary regions
        for (final RegionEnum region : regionProvider.getNonPrimaryRegions()) {
            createAnnounceJobForRegion(region, previousVersion, newVersion, canaryValidationJob, primaryRegionAnnounceJob);
        }

        addDeleteJob(previousVersion, newVersion, publishJobs);

        if(ctx.getConfig().isCreateDevSlicedBlob())
            scheduler.submitJob(jobCreator.createDevSliceJob(ctx, primaryRegionAnnounceJob, inputDataVersion, newVersion));
        
        return new WorkflowCycleStatusFuture(ctx.getStatusIndicator(), newVersion);
    }

    private AnnounceJob createAnnounceJobForRegion(RegionEnum region, long previousVerion, long newVersion, CanaryValidationJob validationJob, AnnounceJob primaryRegionAnnounceJob) {
        DelayJob delayJob = jobCreator.createDelayJob(primaryRegionAnnounceJob, regionProvider.getPublishDelayInSeconds(region) * 1000, newVersion);
        scheduler.submitJob(delayJob);

        AnnounceJob announceJob = jobCreator.createAnnounceJob(vip, previousVerion, newVersion, region, validationJob, delayJob, priorAnnouncedJobs.get(region));
        scheduler.submitJob(announceJob);

        priorAnnouncedJobs.put(region, announceJob);

        if (primaryRegionAnnounceJob == null) {
            // / this is the primary region, create an auto pinback job
            AutoPinbackJob autoPinbackJob = jobCreator.createAutoPinbackJob(announceJob, 300000L, newVersion);
            scheduler.submitJob(autoPinbackJob);
        }

        return announceJob;
    }


    private CanaryValidationJob addCanaryJobs(long previousVersion, long newVersion, CircuitBreakerJob circuitBreakerJob, List<PublicationJob> publishJobs) {
        Map<RegionEnum, BeforeCanaryAnnounceJob> beforeCanaryAnnounceJobs = new HashMap<RegionEnum, BeforeCanaryAnnounceJob>(3);
        Map<RegionEnum, AfterCanaryAnnounceJob> afterCanaryAnnounceJobs = new HashMap<RegionEnum, AfterCanaryAnnounceJob>(3);

        for (RegionEnum region : PublishRegionProvider.ALL_REGIONS) {
            BeforeCanaryAnnounceJob beforeCanaryAnnounceJob = jobCreator.createBeforeCanaryAnnounceJob(vip, newVersion, region, circuitBreakerJob, publishJobs);
            scheduler.submitJob(beforeCanaryAnnounceJob);

            CanaryAnnounceJob canaryAnnounceJob = jobCreator.createCanaryAnnounceJob(vip, newVersion, region, beforeCanaryAnnounceJob);
            scheduler.submitJob(canaryAnnounceJob);

            AfterCanaryAnnounceJob afterCanaryAnnounceJob = jobCreator.createAfterCanaryAnnounceJob(vip, newVersion, region, beforeCanaryAnnounceJob, canaryAnnounceJob);
            scheduler.submitJob(afterCanaryAnnounceJob);

            beforeCanaryAnnounceJobs.put(region, beforeCanaryAnnounceJob);
            afterCanaryAnnounceJobs.put(region, afterCanaryAnnounceJob);
        }

        CanaryValidationJob validationJob = jobCreator.createCanaryValidationJob(vip, newVersion, beforeCanaryAnnounceJobs, afterCanaryAnnounceJobs);
        PoisonStateMarkerJob canaryPoisonStateMarkerJob = jobCreator.createPoisonStateMarkerJob(validationJob, newVersion);
        CanaryRollbackJob canaryRollbackJob = jobCreator.createCanaryRollbackJob(vip, newVersion, previousVersion, validationJob);

        scheduler.submitJob(validationJob);
        scheduler.submitJob(canaryPoisonStateMarkerJob);
        scheduler.submitJob(canaryRollbackJob);

        return validationJob;
    }

    private CircuitBreakerJob addCircuitBreakerJob(long previousVersion, long newVersion) {
        File snapshotFile = new File(fileNamer.getSnapshotFileName(newVersion));
        File deltaFile = new File(fileNamer.getDeltaFileName(previousVersion, newVersion));
        File reverseDeltaFile = new File(fileNamer.getReverseDeltaFileName(newVersion, previousVersion));
        
        File nostreamsSnapshotFile = new File(fileNamer.getNostreamsSnapshotFileName(newVersion));
        File nostreamsDeltaFile = new File(fileNamer.getNostreamsDeltaFileName(previousVersion, newVersion));
        File nostreamsReverseDeltaFile = new File(fileNamer.getNostreamsReverseDeltaFileName(newVersion, previousVersion));

        CircuitBreakerJob validationJob = jobCreator.createCircuitBreakerJob(vip, newVersion, snapshotFile, deltaFile, reverseDeltaFile, nostreamsSnapshotFile, nostreamsDeltaFile, nostreamsReverseDeltaFile);
        scheduler.submitJob(validationJob);

        PoisonStateMarkerJob poisonMarkerJob = jobCreator.createPoisonStateMarkerJob(validationJob, newVersion);
        scheduler.submitJob(poisonMarkerJob);

        return validationJob;
    }

    private void addDeleteJob(long previousVersion, long nextVersion, List<PublicationJob> publishJobsForCycle) {
        scheduler.submitJob(jobCreator.createDeleteFileJob(publishJobsForCycle, nextVersion, 
                fileNamer.getDeltaFileName(previousVersion, nextVersion), 
                fileNamer.getReverseDeltaFileName(nextVersion, previousVersion), 
                fileNamer.getSnapshotFileName(nextVersion),
                fileNamer.getNostreamsDeltaFileName(previousVersion, nextVersion),
                fileNamer.getNostreamsReverseDeltaFileName(nextVersion, previousVersion),
                fileNamer.getNostreamsSnapshotFileName(nextVersion)));
    }

    private List<PublicationJob> addPublishJobs(long inputDataVersion, long previousVersion, long newVersion) {
        File snapshotFile = new File(fileNamer.getSnapshotFileName(newVersion));
        File reverseDeltaFile = new File(fileNamer.getReverseDeltaFileName(newVersion, previousVersion));
        File deltaFile = new File(fileNamer.getDeltaFileName(previousVersion, newVersion));
        File nostreamsSnapshotFile = new File(fileNamer.getNostreamsSnapshotFileName(newVersion));
        File nostreamsReverseDeltaFile = new File(fileNamer.getNostreamsReverseDeltaFileName(newVersion, previousVersion));
        File nostreamsDeltaFile = new File(fileNamer.getNostreamsDeltaFileName(previousVersion, newVersion));

        List<PublicationJob> submittedJobs = new ArrayList<>();
        if (snapshotFile.exists()) {
            HollowBlobPublishJob publishJob = jobCreator.createPublishJob(vip, PublishType.SNAPSHOT, false, inputDataVersion, previousVersion, newVersion, snapshotFile);
            scheduler.submitJob(publishJob);
            submittedJobs.add(publishJob);
            
            publishJob = jobCreator.createPublishJob(vip + "_nostreams", PublishType.SNAPSHOT, true, inputDataVersion, previousVersion, newVersion, nostreamsSnapshotFile);
            scheduler.submitJob(publishJob);
            submittedJobs.add(publishJob);
        }
        if (deltaFile.exists()) {
            HollowBlobPublishJob publishJob = jobCreator.createPublishJob(vip, PublishType.DELTA, false, inputDataVersion, previousVersion, newVersion, deltaFile);
            scheduler.submitJob(publishJob);
            submittedJobs.add(publishJob);
            
            publishJob = jobCreator.createPublishJob(vip + "_nostreams", PublishType.DELTA, true, inputDataVersion, previousVersion, newVersion, nostreamsDeltaFile);
            scheduler.submitJob(publishJob);
            submittedJobs.add(publishJob);
        }
        if (reverseDeltaFile.exists()) {
            HollowBlobPublishJob publishJob = jobCreator.createPublishJob(vip, PublishType.REVERSEDELTA, false, inputDataVersion, previousVersion, newVersion, reverseDeltaFile);
            scheduler.submitJob(publishJob);
            submittedJobs.add(publishJob);
            
            publishJob = jobCreator.createPublishJob(vip + "_nostreams", PublishType.REVERSEDELTA, true, inputDataVersion, previousVersion, newVersion, nostreamsReverseDeltaFile);
            scheduler.submitJob(publishJob);
            submittedJobs.add(publishJob);
        }
        return submittedJobs;

    }

    PublicationJobScheduler getExecutor() {
        return scheduler;
    }

    // TODO: use constructor injection
    void injectPublishRegionProvider(PublishRegionProvider regionProvider) {
        this.regionProvider = regionProvider;
    }

    private void exposePublicationHistory() {
        ctx.getPublicationHistoryConsumer().accept(scheduler.getHistory());
    }

}
