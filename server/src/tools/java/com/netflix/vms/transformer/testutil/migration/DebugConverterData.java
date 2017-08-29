package com.netflix.vms.transformer.testutil.migration;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.HollowConsumer.BlobRetriever;
import com.netflix.hollow.core.index.HollowPrimaryKeyIndex;
import com.netflix.hollow.core.index.key.HollowPrimaryKeyValueDeriver;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.engine.HollowTypeReadState;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.util.HollowWriteStateCreator;
import com.netflix.hollow.core.write.HollowTypeWriteState;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import com.netflix.hollow.history.ui.jetty.HollowHistoryUIServer;
import com.netflix.hollow.tools.stringifier.HollowRecordStringifier;
import com.netflix.hollow.tools.traverse.TransitiveSetTraverser;
import com.netflix.internal.hollow.factory.HollowBlobRetrieverFactory;
import com.netflix.vms.transformer.hollowinput.PackageStreamHollow;
import com.netflix.vms.transformer.hollowinput.StreamDeploymentHollow;
import com.netflix.vms.transformer.hollowinput.StringHollow;
import com.netflix.vms.transformer.hollowinput.VMSHollowInputAPI;
import com.netflix.vms.transformer.input.VMSInputDataClient;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.Test;

public class DebugConverterData {
    private static final String CONVERTER_VIP_NAME = "muon";
    private static final String CONVERTER_NAMESPACE = "vmsconverter-muon";
    private static final String WORKING_DIR = "/space/converter-data/debug";
    private static final String WORKING_DIR_FOR_INPUTCLIENT = "/space/converter-data/inputclient";

    public void setup() throws Exception {
        for (String folder : Arrays.asList(WORKING_DIR, WORKING_DIR_FOR_INPUTCLIENT)) {
            File workingDir = new File(folder);
            if (!workingDir.exists()) workingDir.mkdirs();
        }
    }

    @Test
    public void debugStreamDeploymentS3PathToStreamIds() {
        long version = 20170824034503068L;
        VMSInputDataClient inputClient = new VMSInputDataClient(VMSInputDataClient.PROD_PROXY_URL, WORKING_DIR_FOR_INPUTCLIENT, CONVERTER_VIP_NAME);
        inputClient.triggerRefreshTo(version);

        Map<String, Set<Long>> map = new TreeMap<>();
        VMSHollowInputAPI api = inputClient.getAPI();

        for (PackageStreamHollow stream : api.getAllPackageStreamHollow()) {
            StreamDeploymentHollow deployment = stream._getDeployment();
            if (deployment==null) continue;

            StringHollow s3PathComponent = deployment._getS3PathComponent();
            if (s3PathComponent == null) continue;

            String path = s3PathComponent._getValue();
            Set<Long> ids = map.get(path);
            if (ids==null) {
                ids = new HashSet<>();
                map.put(path, ids);
            }
            ids.add(stream._getDownloadableIdBoxed());
        }

        int i = 1;
        for (Map.Entry<String, Set<Long>> entry : map.entrySet()) {
            System.out.println(String.format("%d) %s = [size=%d] downloadIds=%s", i++, entry.getKey(), entry.getValue().size(), entry.getValue()));
        }

        // FOUND: 1) Celeste Holm (1961-1996) = [size=2] downloadIds=[572674263, 572672107]
    }

    @Test
    public void debugVersionsWithBadPackageStream() {
        long[] versions = new long[] { 20170824030803390L, 20170824030803390L, 20170824031131543L, 20170824031347430L, 20170824031546123L, 20170824032458038L, 20170824032722275L, 20170824032941389L, 20170824033200595L, 20170824033533733L, 20170824033754309L, 20170824034009909L, 20170824034238345L, 20170824034503068L };
        HollowRecordStringifier stringifier = new HollowRecordStringifier(true, true, false);

        // FOUND: 1) Celeste Holm (1961-1996) = [size=2] downloadIds=[572674263, 572672107]
        int i = 1;
        long badDownloadableId = 572674263L; // long badDownloadableId2 = 572672107L;
        VMSInputDataClient inputClient = new VMSInputDataClient(VMSInputDataClient.PROD_PROXY_URL, WORKING_DIR_FOR_INPUTCLIENT, CONVERTER_VIP_NAME);
        for (long version : versions) {
            inputClient.triggerRefreshTo(version);
            HollowPrimaryKeyIndex index = new HollowPrimaryKeyIndex(inputClient.getStateEngine(), "PackageStream", "downloadableId");
            int ordinal = index.getMatchingOrdinal(badDownloadableId);

            VMSHollowInputAPI api = inputClient.getAPI();
            PackageStreamHollow stream = api.getPackageStreamHollow(ordinal);
            if (stream._getDeployment()._getS3PathComponent() != null) {
                System.out.println(String.format("%d) version=%s, dId=%s, deployment=%s", i++, version, stream._getDownloadableId(), stringifier.stringify(stream._getDeployment())));
            }
        }

        /*
         * VERSIONS:
         * 1) version=20170824033754309, dId=572674263, deployment=(StreamDeployment) (ordinal 2345573)
         * 2) version=20170824034009909, dId=572674263, deployment=(StreamDeployment) (ordinal 2345573)
         * 3) version=20170824034238345, dId=572674263, deployment=(StreamDeployment) (ordinal 2345573)
         * 4) version=20170824034503068, dId=572674263, deployment=(StreamDeployment) (ordinal 2345573)
         */
    }

    @Test
    public void debugConverterWithHistory() throws Exception {
        long version = 20170824030803390L;
        long toVersion = 20170824034503068L;

        // FOUND: 1) Celeste Holm (1961-1996) = [size=2] downloadIds=[572674263, 572672107]
        BlobRetriever blobRetriever = HollowBlobRetrieverFactory.localProxyForProdEnvironment().getForNamespace(CONVERTER_NAMESPACE);
        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobRetriever).withLocalBlobStore(new File(WORKING_DIR)).build();
        HollowHistoryUIServer historyUI = new HollowHistoryUIServer(consumer, 7777);
        historyUI.start();
        consumer.triggerRefreshTo(version);
        consumer.triggerRefreshTo(toVersion);
        historyUI.join();
    }

    @Test
    public void walkthroughConverterVersionsWithHollowConsumer() throws Exception {
        long[] versions = new long[] { 20170824030803390L, 20170824030803390L, 20170824031131543L, 20170824031347430L, 20170824031546123L, 20170824032458038L, 20170824032722275L, 20170824032941389L, 20170824033200595L, 20170824033533733L, 20170824033754309L, 20170824034009909L, 20170824034238345L, 20170824034503068L };

        int badPackageStreamOrdinal = 54076780;
        BlobRetriever blobRetriever = HollowBlobRetrieverFactory.localProxyForProdEnvironment().getForNamespace(CONVERTER_NAMESPACE);
        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobRetriever).withLocalBlobStore(new File(WORKING_DIR)).withGeneratedAPIClass(VMSHollowInputAPI.class).build();

        for (long version : versions) {
            System.out.println("Refreshing to version=" + version);
            consumer.triggerRefreshTo(version);
            VMSHollowInputAPI api = (VMSHollowInputAPI) consumer.getAPI();

            BitSet ordinals = consumer.getStateEngine().getTypeState("StreamDeployment").getPopulatedOrdinals();
            System.out.println("\t StreamDeployment ordinal 2345573 exists? " + ordinals.get(2345573));

            PackageStreamHollow stream = api.getPackageStreamHollow(badPackageStreamOrdinal);

            StreamDeploymentHollow deployment = stream._getDeployment();
            System.out.println("\t DeploymentInfo ordinal=" + deployment._getDeploymentInfo().getOrdinal());
            //            System.out.println(stringifier.stringify(stream));

            if (deployment._getS3FullPath() != null) {
                StringHollow hStr = deployment._getS3FullPath();
                System.out.println(String.format("\t s3FullPath=%s, ordinal=%d", hStr._getValue(), hStr.getOrdinal()));
            }

            if (deployment._getS3PathComponent() != null) {
                StringHollow hStr = deployment._getS3PathComponent();
                System.out.println(String.format("\t s3PathComponent=%s, ordinal=%d", hStr._getValue(), hStr.getOrdinal()));
            }
        }
    }

    @Test
    public void debugConverterBeforeAndAfter() throws Exception {
        HollowRecordStringifier stringifier = new HollowRecordStringifier(true, true, false);
        BlobRetriever blobRetriever = HollowBlobRetrieverFactory.localProxyForProdEnvironment().getForNamespace(CONVERTER_NAMESPACE);
        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobRetriever).withLocalBlobStore(new File(WORKING_DIR)).withGeneratedAPIClass(VMSHollowInputAPI.class).build();

        // FOUND: 1) Celeste Holm (1961-1996) = [size=2] downloadIds=[572674263, 572672107]
        long badDownloadableId = 572674263L;
        int fishyStreamDeploymentOrdinal = 2345573;
        {
            consumer.triggerRefreshTo(20170824033200595L);
            System.out.println("\n\n---- FIRST STATE ---- version=" + consumer.getCurrentVersionId());
            BitSet ordinals = consumer.getStateEngine().getTypeState("StreamDeployment").getPopulatedOrdinals();
            System.out.println("\t StreamDeployment ordinal 2345573 exists? " + ordinals.get(fishyStreamDeploymentOrdinal));

            HollowPrimaryKeyIndex index = new HollowPrimaryKeyIndex(consumer.getStateEngine(), "PackageStream", "downloadableId");
            int ordinal = index.getMatchingOrdinal(badDownloadableId);
            //GenericHollowRecordHelper.instantiate(rEngine, type, ordinal);
            System.out.println(stringifier.stringify(consumer.getStateEngine(), "PackageStream", ordinal));
        }

        {
            consumer.triggerRefreshTo(20170824033533733L); // This state already seems to be bad since STreamDeployment ordinal is already removed
            System.out.println("\n\n---- BEFORE SUSPECIOUS UPADATE ---- version=" + consumer.getCurrentVersionId());
            BitSet ordinals = consumer.getStateEngine().getTypeState("StreamDeployment").getPopulatedOrdinals();
            System.out.println("\t StreamDeployment ordinal 2345573 exists? " + ordinals.get(fishyStreamDeploymentOrdinal));

            HollowPrimaryKeyIndex index = new HollowPrimaryKeyIndex(consumer.getStateEngine(), "PackageStream", "downloadableId");
            int ordinal = index.getMatchingOrdinal(badDownloadableId);
            System.out.println(stringifier.stringify(consumer.getStateEngine(), "PackageStream", ordinal));
        }

        {
            consumer.triggerRefreshTo(20170824033754309L); // First State with S3Path = Celeste Holm (1961-1996)
            System.out.println("\n\n---- AFTER SUSPECIOUS UPADATE ---- version=" + consumer.getCurrentVersionId());
            BitSet ordinals = consumer.getStateEngine().getTypeState("StreamDeployment").getPopulatedOrdinals();
            System.out.println("\t StreamDeployment ordinal 2345573 exists? " + ordinals.get(fishyStreamDeploymentOrdinal));

            HollowPrimaryKeyIndex index = new HollowPrimaryKeyIndex(consumer.getStateEngine(), "PackageStream", "downloadableId");
            int ordinal = index.getMatchingOrdinal(badDownloadableId);
            System.out.println(stringifier.stringify(consumer.getStateEngine(), "PackageStream", ordinal));
        }
    }

    @Test
    public void reproduceConverterIssueSimulatingEvents() throws Exception {
        BlobRetriever blobRetriever = HollowBlobRetrieverFactory.localProxyForProdEnvironment().getForNamespace(CONVERTER_NAMESPACE);

        long goodStateVersion = 20170824033200595L;
        long suspeciousStateVersion = 20170824033533733L;
        HollowConsumer consumerState1 = HollowConsumer.withBlobRetriever(blobRetriever).withLocalBlobStore(new File(WORKING_DIR)).withGeneratedAPIClass(VMSHollowInputAPI.class).build();
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                consumerState1.triggerRefreshTo(goodStateVersion);
            }
        });
        t1.start();

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobRetriever).withLocalBlobStore(new File(WORKING_DIR)).withGeneratedAPIClass(VMSHollowInputAPI.class).build();
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                consumer.triggerRefreshTo(goodStateVersion);
            }
        });
        t2.start();

        t1.join();
        t2.join();

        HollowReadStateEngine rEngine = consumer.getStateEngine();
        HollowWriteStateEngine wEngine = HollowWriteStateCreator.recreateAndPopulateUsingReadEngine(rEngine);

        wEngine.prepareForWrite();
        wEngine.prepareForNextCycle();

        consumer.triggerRefreshTo(suspeciousStateVersion);
        HollowTypeReadState typeState = consumer.getStateEngine().getTypeState("Package");
        BitSet populatedOrdinals = typeState.getPopulatedOrdinals();
        BitSet previousOrdinals = typeState.getPreviousOrdinals();
        { // Simulate Converter in removing records from state engine that are in events
            BitSet modifiedSet = new BitSet();
            modifiedSet.or(populatedOrdinals);
            modifiedSet.xor(previousOrdinals);

            HollowPrimaryKeyValueDeriver valDeriver = new HollowPrimaryKeyValueDeriver(((HollowObjectSchema) consumer.getStateEngine().getSchema("Package")).getPrimaryKey(), consumer.getStateEngine());
            int o = modifiedSet.nextSetBit(0);
            List<Object[]> modifiedKeys = new ArrayList<>();
            while (o != -1) {
                Object[] recordKey = valDeriver.getRecordKey(o);
                modifiedKeys.add(recordKey);

                o = modifiedSet.nextSetBit(o + 1);
            }

            removeEventsData(consumerState1.getStateEngine(), wEngine, modifiedKeys);
        }


        { // Simulate Converter in adding events to blob
            BitSet newDataSet = new BitSet();
            int o = populatedOrdinals.nextSetBit(0);
            while (o != -1) {
                if (!previousOrdinals.get(o)) {
                    // new records
                    newDataSet.set(o);
                }
                o = populatedOrdinals.nextSetBit(o + 1);
            }
            // @TODO need to add the data with ordinals in newDataSet to write state engine
        }

        int badStreamDeploymentOrdinal = 2345573;
        // @TODO then look at SteamDeployment with badStreamDeploymentOrdinal and see if it was able to reproduce issue
    }

    // Ported from ConverterStateEngine.removeEventsData
    private void removeEventsData(HollowReadStateEngine rEngine, HollowWriteStateEngine wEngine, List<Object[]> modifiedKeys) {
        HollowPrimaryKeyIndex coldstartIdx = new HollowPrimaryKeyIndex(rEngine, ((HollowObjectSchema) rEngine.getSchema("Package")).getPrimaryKey());

        BitSet eventOverriddenTypeOrdinals = new BitSet();
        Map<String, BitSet> ordinalsToRemove = new HashMap<>();
        for (Object[] recKey : modifiedKeys) {
            int ordinal = coldstartIdx.getMatchingOrdinal(recKey);
            if (ordinal != -1)
                eventOverriddenTypeOrdinals.set(ordinal);
        }
        ordinalsToRemove.put("Package", eventOverriddenTypeOrdinals);

        TransitiveSetTraverser.addTransitiveMatches(rEngine, ordinalsToRemove);
        TransitiveSetTraverser.removeReferencedOutsideClosure(rEngine, ordinalsToRemove);
        removeOrdinalsFromThisCycle(wEngine, ordinalsToRemove);
    }

    // Ported from ConverterStateEngine.removeOrdinalsFromThisCycle
    private void removeOrdinalsFromThisCycle(HollowWriteStateEngine wEngine, Map<String, BitSet> recordsToRemove) {
        recordsToRemove.entrySet().stream().forEach(entry -> {
            HollowTypeWriteState typeWriteState = wEngine.getTypeState(entry.getKey());
            int ordinal = entry.getValue().nextSetBit(0);
            while (ordinal != -1) {
                typeWriteState.removeOrdinalFromThisCycle(ordinal);
                ordinal = entry.getValue().nextSetBit(ordinal + 1);
            }
        });
    }
}