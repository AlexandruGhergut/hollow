package com.netflix.vms.transformer.modules.countryspecific;

import com.netflix.hollow.core.index.HollowPrimaryKeyIndex;
import com.netflix.vms.transformer.common.TransformerContext;
import com.netflix.vms.transformer.data.CupTokenFetcher;
import com.netflix.vms.transformer.data.DeployablePackagesFetcher;
import com.netflix.vms.transformer.hollowinput.ContractHollow;
import com.netflix.vms.transformer.hollowinput.PackageHollow;
import com.netflix.vms.transformer.hollowinput.RightsWindowContractHollow;
import com.netflix.vms.transformer.hollowinput.VMSHollowInputAPI;
import com.netflix.vms.transformer.hollowinput.VideoGeneralHollow;
import com.netflix.vms.transformer.hollowoutput.LinkedHashSetOfStrings;
import com.netflix.vms.transformer.hollowoutput.PackageData;
import com.netflix.vms.transformer.hollowoutput.Strings;
import com.netflix.vms.transformer.hollowoutput.VideoContractInfo;
import com.netflix.vms.transformer.hollowoutput.VideoPackageInfo;
import com.netflix.vms.transformer.hollowoutput.WindowPackageContractInfo;
import com.netflix.vms.transformer.index.IndexSpec;
import com.netflix.vms.transformer.index.VMSTransformerIndexer;
import com.netflix.vms.transformer.modules.packages.PackageDataCollection;

import java.util.Collections;
import java.util.stream.Collectors;

public class WindowPackageContractInfoModule {

    private final VMSHollowInputAPI api;
    private final TransformerContext ctx;
    private final HollowPrimaryKeyIndex packageIdx;
    private final HollowPrimaryKeyIndex videoGeneralIdx;

    private final CupTokenFetcher cupTokenFetcher;
    private final DeployablePackagesFetcher deployablePackagesFetcher;
    private final PackageMomentDataModule packageMomentDataModule;
    private final VideoPackageInfo FILTERED_VIDEO_PACKAGE_INFO;

    public WindowPackageContractInfoModule(VMSHollowInputAPI api, VMSTransformerIndexer indexer,
            CupTokenFetcher cupTokenFetcher, DeployablePackagesFetcher deployablePackagesFetcher,
            TransformerContext ctx) {
        this.api = api;
        this.ctx = ctx;
        this.cupTokenFetcher = cupTokenFetcher;
        this.deployablePackagesFetcher = deployablePackagesFetcher;
        this.packageMomentDataModule = new PackageMomentDataModule(ctx.getConfig());
        this.packageIdx = indexer.getPrimaryKeyIndex(IndexSpec.PACKAGES);
        this.videoGeneralIdx = indexer.getPrimaryKeyIndex(IndexSpec.VIDEO_GENERAL);
        FILTERED_VIDEO_PACKAGE_INFO = newEmptyVideoPackageInfo();
    }

    public WindowPackageContractInfo buildWindowPackageContractInfo(int videoId, PackageData packageData,
            RightsWindowContractHollow windowContractHollow, ContractHollow contract, String country, 
            boolean isAvailableForDownload, PackageDataCollection packageDataCollection) {
        PackageHollow inputPackage = api.getPackageHollow(packageIdx.getMatchingOrdinal((long) packageData.id));


        // create contract info
        WindowPackageContractInfo info = new WindowPackageContractInfo();
        info.videoContractInfo = new VideoContractInfo();
        info.videoContractInfo.contractId = (int) windowContractHollow._getDealId();
        info.videoContractInfo.isAvailableForDownload = isAvailableForDownload;
        info.videoContractInfo.primaryPackageId = (int) windowContractHollow._getPackageId();
        assignContractInfo(info, contract, videoId);
        info.videoContractInfo.assetBcp47Codes = windowContractHollow._getAssets().stream().map(a -> new Strings(a._getBcp47Code()._getValue().toCharArray())).collect(Collectors.toSet());

        // create package info
        info.videoPackageInfo = newEmptyVideoPackageInfo();
        info.videoPackageInfo.packageId = packageData.id;
        info.videoPackageInfo.isDefaultPackage = deployablePackagesFetcher.isDefaultPackage(
                (long) packageData.id, videoId);

        PackageMomentData packageMomentData = packageMomentDataModule.getWindowPackageMomentData(packageData, inputPackage, ctx);
        info.videoPackageInfo.startMomentOffsetInMillis = packageMomentData.startMomentOffsetInMillis;
        info.videoPackageInfo.endMomentOffsetInMillis = packageMomentData.endMomentOffsetInMillis;
        info.videoPackageInfo.timecodes = packageMomentData.timecodes;

        info.videoPackageInfo.trickPlayMap = packageDataCollection.getTrickPlayItemMap();
        info.videoPackageInfo.formats = packageDataCollection.getVideoDescriptorFormats();
        info.videoPackageInfo.screenFormats = packageDataCollection.getScreenFormats();
        info.videoPackageInfo.soundTypes = packageDataCollection.getSoundTypes(country);
        info.videoPackageInfo.runtimeInSeconds = (int) packageDataCollection.getLongestRuntimeInSeconds();

        return info;
    }


    public WindowPackageContractInfo buildWindowPackageContractInfoWithoutPackage(int packageId, RightsWindowContractHollow windowContractHollow, 
    		ContractHollow contract, int videoId) {
        WindowPackageContractInfo info = new WindowPackageContractInfo();
        info.videoContractInfo = new VideoContractInfo();
        info.videoContractInfo.contractId = (int) windowContractHollow._getDealId();
        info.videoContractInfo.primaryPackageId = packageId;
        assignContractInfo(info, contract, videoId);
        info.videoContractInfo.assetBcp47Codes = windowContractHollow._getAssets().stream().map(a -> new Strings(a._getBcp47Code()._getValue().toCharArray())).collect(Collectors.toSet());
        info.videoPackageInfo = getFilteredVideoPackageInfo(videoId, packageId);
        return info;
    }

    private void assignContractInfo(WindowPackageContractInfo info, ContractHollow contract, int videoId) {
        if (contract != null) {
            if (contract._getPrePromotionDays() != Long.MIN_VALUE)
                info.videoContractInfo.prePromotionDays = (int) contract._getPrePromotionDays();
            info.videoContractInfo.isDayOfBroadcast = contract._getDayOfBroadcast();
            info.videoContractInfo.isDayAfterBroadcast = contract._getDayAfterBroadcast();
            info.videoContractInfo.hasRollingEpisodes = contract._getDayAfterBroadcast(); // NOTE: DAB and hasRollingEpisodes means the same
            info.videoContractInfo.cupTokens = new LinkedHashSetOfStrings(Collections.singletonList(
            		cupTokenFetcher.getCupToken(videoId, contract)));
        } else {
            info.videoContractInfo.cupTokens = new LinkedHashSetOfStrings(Collections.emptyList());
        }
    }

    public WindowPackageContractInfo buildFilteredWindowPackageContractInfo(int contractId, int videoId) {
        WindowPackageContractInfo info = new WindowPackageContractInfo();
        info.videoContractInfo = getFilteredVideoContractInfo(contractId);
        info.videoPackageInfo = getFilteredVideoPackageInfo(videoId);
        return info;
    }

    private VideoContractInfo getFilteredVideoContractInfo(int contractId) {
        VideoContractInfo info = new VideoContractInfo();
        info.contractId = contractId;
        info.primaryPackageId = 0;
        info.cupTokens = new LinkedHashSetOfStrings();
        info.cupTokens.ordinals = Collections.emptyList();
        info.assetBcp47Codes = Collections.emptySet();
        return info;
    }

    private int getApproximateRuntimeInSecods(long videoId) {
        int ordinal = videoGeneralIdx.getMatchingOrdinal(videoId);
        VideoGeneralHollow general = api.getVideoGeneralHollow(ordinal);
        if (general != null)
            return (int) general._getRuntime();
        return 0;
    }

    private VideoPackageInfo getFilteredVideoPackageInfo(long videoId) {
        int approxRuntimeInSecs = getApproximateRuntimeInSecods(videoId);
        if (approxRuntimeInSecs == 0) return FILTERED_VIDEO_PACKAGE_INFO;

        VideoPackageInfo result = newEmptyVideoPackageInfo();
        result.runtimeInSeconds = approxRuntimeInSecs;
        return result;
    }

    private VideoPackageInfo getFilteredVideoPackageInfo(long videoId, int packageId) {
        VideoPackageInfo result = newEmptyVideoPackageInfo();
        result.packageId = packageId;
        result.runtimeInSeconds = getApproximateRuntimeInSecods(videoId);
        return result;
    }

    static VideoPackageInfo newEmptyVideoPackageInfo() {
        VideoPackageInfo info = new VideoPackageInfo();
        info.packageId = 0;
        info.runtimeInSeconds = 0;
        info.soundTypes = Collections.emptyList();
        info.screenFormats = Collections.emptyList();
        info.trickPlayMap = Collections.emptyMap();
        info.formats = Collections.emptySet();
        return info;

    }

    public void reset() {
        this.packageMomentDataModule.reset();
    }

}
