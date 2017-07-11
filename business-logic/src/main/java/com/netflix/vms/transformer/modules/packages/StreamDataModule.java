package com.netflix.vms.transformer.modules.packages;

import com.netflix.hollow.core.index.HollowPrimaryKeyIndex;
import com.netflix.hollow.core.write.objectmapper.HollowObjectMapper;
import com.netflix.vms.transformer.CycleConstants;
import com.netflix.vms.transformer.common.TransformerContext;
import com.netflix.vms.transformer.common.io.TransformerLogTag;
import com.netflix.vms.transformer.hollowinput.AudioStreamInfoHollow;
import com.netflix.vms.transformer.hollowinput.CdnDeploymentHollow;
import com.netflix.vms.transformer.hollowinput.ISOCountryHollow;
import com.netflix.vms.transformer.hollowinput.ImageStreamInfoHollow;
import com.netflix.vms.transformer.hollowinput.PackageHollow;
import com.netflix.vms.transformer.hollowinput.PackageStreamHollow;
import com.netflix.vms.transformer.hollowinput.StreamAssetMetadataHollow;
import com.netflix.vms.transformer.hollowinput.StreamAssetTypeHollow;
import com.netflix.vms.transformer.hollowinput.StreamDeploymentHollow;
import com.netflix.vms.transformer.hollowinput.StreamDeploymentInfoHollow;
import com.netflix.vms.transformer.hollowinput.StreamDeploymentLabelHollow;
import com.netflix.vms.transformer.hollowinput.StreamDimensionsHollow;
import com.netflix.vms.transformer.hollowinput.StreamFileIdentificationHollow;
import com.netflix.vms.transformer.hollowinput.StreamNonImageInfoHollow;
import com.netflix.vms.transformer.hollowinput.StreamProfilesHollow;
import com.netflix.vms.transformer.hollowinput.StringHollow;
import com.netflix.vms.transformer.hollowinput.TextStreamInfoHollow;
import com.netflix.vms.transformer.hollowinput.VMSHollowInputAPI;
import com.netflix.vms.transformer.hollowinput.VideoStreamCropParamsHollow;
import com.netflix.vms.transformer.hollowinput.VideoStreamInfoHollow;
import com.netflix.vms.transformer.hollowoutput.AssetMetaData;
import com.netflix.vms.transformer.hollowoutput.AssetTypeDescriptor;
import com.netflix.vms.transformer.hollowoutput.DownloadDescriptor;
import com.netflix.vms.transformer.hollowoutput.DownloadLocation;
import com.netflix.vms.transformer.hollowoutput.DownloadLocationSet;
import com.netflix.vms.transformer.hollowoutput.DownloadableId;
import com.netflix.vms.transformer.hollowoutput.DrmInfo;
import com.netflix.vms.transformer.hollowoutput.DrmInfoData;
import com.netflix.vms.transformer.hollowoutput.DrmKey;
import com.netflix.vms.transformer.hollowoutput.FrameRate;
import com.netflix.vms.transformer.hollowoutput.ISOCountry;
import com.netflix.vms.transformer.hollowoutput.ImageSubtitleIndexByteRange;
import com.netflix.vms.transformer.hollowoutput.PixelAspect;
import com.netflix.vms.transformer.hollowoutput.QoEInfo;
import com.netflix.vms.transformer.hollowoutput.StreamAdditionalData;
import com.netflix.vms.transformer.hollowoutput.StreamCropParams;
import com.netflix.vms.transformer.hollowoutput.StreamData;
import com.netflix.vms.transformer.hollowoutput.StreamDataDescriptor;
import com.netflix.vms.transformer.hollowoutput.StreamDownloadLocationFilename;
import com.netflix.vms.transformer.hollowoutput.StreamDrmData;
import com.netflix.vms.transformer.hollowoutput.StreamMostlyConstantData;
import com.netflix.vms.transformer.hollowoutput.Strings;
import com.netflix.vms.transformer.hollowoutput.TargetDimensions;
import com.netflix.vms.transformer.hollowoutput.TimedTextTypeDescriptor;
import com.netflix.vms.transformer.hollowoutput.VideoFormatDescriptor;
import com.netflix.vms.transformer.hollowoutput.VideoResolution;
import com.netflix.vms.transformer.hollowoutput.WmDrmKey;
import com.netflix.vms.transformer.index.IndexSpec;
import com.netflix.vms.transformer.index.VMSTransformerIndexer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StreamDataModule {

    private final StreamDrmData EMPTY_DRM_DATA = new StreamDrmData();
    private final DownloadLocationSet EMPTY_DOWNLOAD_LOCATIONS = new DownloadLocationSet();

    private final Map<String, AssetTypeDescriptor> assetTypeDescriptorMap;
    private final Map<String, TimedTextTypeDescriptor> timedTextTypeDescriptorMap;
    private final Map<String, Integer> deploymentLabelBitsetOffsetMap;
    private final Map<String, List<Strings>> tagsLists;
    private final Map<Integer, Object> drmKeysByGroupId;
    private final Map<Integer, DrmInfo> drmInfoByGroupId;

    private final VideoFormatDescriptorIdentifier videoFormatIdentifier;
    private final HollowPrimaryKeyIndex streamProfileIdx;
    private final VMSHollowInputAPI api;
    private final TransformerContext ctx;
    private final CycleConstants cycleConstants;
    private final HollowObjectMapper objectMapper;

    public StreamDataModule(VMSHollowInputAPI api, TransformerContext ctx, CycleConstants cycleConstants, VMSTransformerIndexer indexer, HollowObjectMapper objectMapper, Map<Integer, Object> drmKeysByGroupId, Map<Integer, DrmInfo> drmInfoByGroupId) {
        this.api = api;
        this.ctx = ctx;
        this.cycleConstants = cycleConstants;
        this.assetTypeDescriptorMap = getAssetTypeDescriptorMap();
        this.timedTextTypeDescriptorMap = getTimedTextTypeDescriptorMap();
        this.deploymentLabelBitsetOffsetMap = getDeploymentLabelBitsetOffsetMap();
        this.tagsLists = new HashMap<String, List<Strings>>();
        this.drmKeysByGroupId = drmKeysByGroupId;
        this.drmInfoByGroupId = drmInfoByGroupId;

        this.streamProfileIdx = indexer.getPrimaryKeyIndex(IndexSpec.STREAM_PROFILE);

        this.videoFormatIdentifier = new VideoFormatDescriptorIdentifier(api, ctx, cycleConstants, indexer);

        /// only necessary for rogue DrmKeys.
        this.objectMapper = objectMapper;

        EMPTY_DOWNLOAD_LOCATIONS.filename = new StreamDownloadLocationFilename("");
        EMPTY_DOWNLOAD_LOCATIONS.locations = Collections.emptyList();
    }

    StreamData convertStreamData(PackageHollow packages, PackageStreamHollow inputStream, DrmInfoData drmInfoData) {
        int encodingProfileId = (int) inputStream._getStreamProfileId();
        int streamProfileOrdinal = streamProfileIdx.getMatchingOrdinal(Long.valueOf(encodingProfileId));
        StreamProfilesHollow streamProfile = api.getStreamProfilesHollow(streamProfileOrdinal);
        if(streamProfile == null)
            return null;

        if(streamProfile._getProfileType()._isValueEqual("MERCHSTILL")) {
            return null;
        }

        ImageStreamInfoHollow inputStreamImageInfo = inputStream._getImageInfo();
        StreamFileIdentificationHollow inputStreamIdentity = inputStream._getFileIdentification();
        StreamDeploymentHollow inputStreamDeployment = inputStream._getDeployment();
        StreamDimensionsHollow inputStreamDimensions = inputStream._getDimensions();
        StreamNonImageInfoHollow inputNonImageInfo = inputStream._getNonImageInfo();
        AudioStreamInfoHollow inputAudioStreamInfo = inputNonImageInfo._getAudioInfo();
        VideoStreamInfoHollow inputVideoStreamInfo = inputNonImageInfo._getVideoInfo();
        TextStreamInfoHollow inputTextStreamInfo = inputNonImageInfo._getTextInfo();

        StreamData outputStream = new StreamData();

        outputStream.downloadableId = new DownloadableId(inputStream._getDownloadableId());
        outputStream.packageId = (int)packages._getPackageId();

        outputStream.fileSizeInBytes = inputStreamIdentity._getFileSizeInBytes();
        outputStream.creationTimeStampInSeconds = inputStreamIdentity._getCreatedTimeSeconds();
        outputStream.cRC32Hash = inputStreamIdentity._getCrc32();
        outputStream.sha1_1 = inputStreamIdentity._getSha1_1();
        outputStream.sha1_2 = inputStreamIdentity._getSha1_2();
        outputStream.sha1_3 = inputStreamIdentity._getSha1_3();

        outputStream.drmData = EMPTY_DRM_DATA;
        outputStream.additionalData = new StreamAdditionalData();
        outputStream.additionalData.mostlyConstantData = new StreamMostlyConstantData();
        outputStream.additionalData.mostlyConstantData.tags = Collections.emptyList();

        if(!Float.isNaN(inputVideoStreamInfo._getFps()))
            outputStream.additionalData.frameRate = new FrameRate(inputVideoStreamInfo._getFps());
        outputStream.additionalData.qoeInfo = new QoEInfo();
        if(inputVideoStreamInfo._getScaledPsnrTimesHundred() != Long.MIN_VALUE)
            outputStream.additionalData.qoeInfo.scaledPsnrScore = (int)inputVideoStreamInfo._getScaledPsnrTimesHundred();
        if(inputVideoStreamInfo._getVmafScore() != Long.MIN_VALUE)
            outputStream.additionalData.qoeInfo.vmafScore = (int)inputVideoStreamInfo._getVmafScore();
        outputStream.additionalData.qoeInfo.vmafAlgoVersionExp = inputVideoStreamInfo._getVmafAlgoVersionExp();
        outputStream.additionalData.qoeInfo.vmafAlgoVersionLts = inputVideoStreamInfo._getVmafAlgoVersionLts();
        outputStream.additionalData.qoeInfo.vmafScoreExp = inputVideoStreamInfo._getVmafScoreExp();
        outputStream.additionalData.qoeInfo.vmafScoreLts = inputVideoStreamInfo._getVmafScoreLts();
        outputStream.additionalData.qoeInfo.vmafplusScoreExp = inputVideoStreamInfo._getVmafplusScoreExp();
        outputStream.additionalData.qoeInfo.vmafplusScoreLts = inputVideoStreamInfo._getVmafplusScoreLts();
        outputStream.additionalData.qoeInfo.vmafplusPhoneScoreExp = inputVideoStreamInfo._getVmafplusPhoneScoreExp();
        outputStream.additionalData.qoeInfo.vmafplusPhoneScoreLts = inputVideoStreamInfo._getVmafplusPhoneScoreLts();

        VideoStreamCropParamsHollow inputCropParams = inputVideoStreamInfo._getCropParams();
        if(inputCropParams != null) {
            outputStream.additionalData.cropParams = new StreamCropParams();
            outputStream.additionalData.cropParams.x = inputCropParams._getX();
            outputStream.additionalData.cropParams.y = inputCropParams._getY();
            outputStream.additionalData.cropParams.width = inputCropParams._getWidth();
            outputStream.additionalData.cropParams.height = inputCropParams._getHeight();
        }

        Set<StreamDeploymentLabelHollow> deploymentLabels = inputStreamDeployment._getDeploymentLabel();
        int deploymentLabelBits = 0;
        if(deploymentLabels != null) {
            for(StreamDeploymentLabelHollow label : deploymentLabels) {
                Integer labelBit = deploymentLabelBitsetOffsetMap.get(label._getValue()._getValue());
                if(labelBit != null)
                    deploymentLabelBits |= (1 << labelBit.intValue());
            }
        }
        outputStream.additionalData.mostlyConstantData.deploymentLabel = deploymentLabelBits;
        outputStream.additionalData.mostlyConstantData.deploymentPriority = inputStreamDeployment._getDeploymentPriority() == Integer.MIN_VALUE ? 300 : inputStreamDeployment._getDeploymentPriority();

        StringHollow tags = inputStream._getTags();
        if(tags != null) {
            outputStream.additionalData.mostlyConstantData.tags = getTagsList(tags._getValue());
        }

        outputStream.additionalData.downloadLocations = EMPTY_DOWNLOAD_LOCATIONS;

        outputStream.streamDataDescriptor = new StreamDataDescriptor();
        outputStream.streamDataDescriptor.cacheDeployedCountries = Collections.emptySet();
        outputStream.downloadDescriptor = new DownloadDescriptor();

        StreamDeploymentInfoHollow deploymentInfo = inputStreamDeployment._getDeploymentInfo();
        if(deploymentInfo != null) {
            Set<ISOCountryHollow> cacheDeployedCountries = deploymentInfo._getCacheDeployedCountries();
            if(cacheDeployedCountries != null) {
                outputStream.streamDataDescriptor.cacheDeployedCountries = new HashSet<ISOCountry>();
                for(ISOCountryHollow country : cacheDeployedCountries) {
                    outputStream.streamDataDescriptor.cacheDeployedCountries.add(cycleConstants.getISOCountry(country._getValue()));
                }
            }

            Set<CdnDeploymentHollow> cdnDeployments = deploymentInfo._getCdnDeployments();
            if(cdnDeployments != null) {
                if(cdnDeployments.size() > 0) {
                    outputStream.additionalData.downloadLocations = new DownloadLocationSet();
                    outputStream.additionalData.downloadLocations.filename = new StreamDownloadLocationFilename(inputStreamIdentity._getFilename());
                    outputStream.additionalData.downloadLocations.locations = new ArrayList<DownloadLocation>();

                    for(CdnDeploymentHollow cdnDeployment : cdnDeployments) {
                        DownloadLocation location = new DownloadLocation();
                        location.directory = new Strings(cdnDeployment._getDirectory()._getValue());
                        location.originServerName = new Strings(cdnDeployment._getOriginServer()._getValue());

                        outputStream.additionalData.downloadLocations.locations.add(location);
                    }
                }
            }
        }

        StringHollow conformingGroupId = inputStreamDeployment._getS3PathComponent();
        if(conformingGroupId != null) {
            outputStream.additionalData.mostlyConstantData.conformingGroupId = Integer.parseInt(conformingGroupId._getValue());
        }

        StringHollow s3FullPath = inputStreamDeployment._getS3FullPath();
        if(s3FullPath != null) {
            outputStream.additionalData.mostlyConstantData.s3FullPath = s3FullPath._getValue().toCharArray();
        }

        int pixelAspectHeight = inputStreamDimensions._getPixelAspectRatioHeight();
        if(pixelAspectHeight != Integer.MIN_VALUE) {
            outputStream.streamDataDescriptor.pixelAspect = new PixelAspect();
            outputStream.streamDataDescriptor.pixelAspect.height = pixelAspectHeight;
            outputStream.streamDataDescriptor.pixelAspect.width = inputStreamDimensions._getPixelAspectRatioWidth();
        }

        int videoResolutionHeight = inputStreamDimensions._getHeightInPixels();
        if(videoResolutionHeight != Integer.MIN_VALUE) {
            outputStream.streamDataDescriptor.videoResolution = new VideoResolution();
            outputStream.streamDataDescriptor.videoResolution.height = videoResolutionHeight;
            outputStream.streamDataDescriptor.videoResolution.width = inputStreamDimensions._getWidthInPixels();
        }
        outputStream.streamDataDescriptor.imageCount = 0;


        StreamAssetTypeHollow inputAssetType = inputStream._getAssetType();
        if(inputAssetType != null && inputAssetType._getAssetType() != null) {
            outputStream.downloadDescriptor.assetTypeDescriptor = assetTypeDescriptorMap.get(inputAssetType._getAssetType()._getValue());
        } else {
            outputStream.downloadDescriptor.assetTypeDescriptor = assetTypeDescriptorMap.get("primary");
        }

        StreamAssetMetadataHollow assetMetadataId = inputStream._getMetadataId();
        if(assetMetadataId != null) {
            String id = assetMetadataId._getId();
            if(id != null) {
                outputStream.downloadDescriptor.assetMetaData = new AssetMetaData(new Strings(id));
            }
        }



        if(inputNonImageInfo != null) {
            outputStream.streamDataDescriptor.runTimeInSeconds = (int) inputNonImageInfo._getRuntimeSeconds();
        }



        if(inputVideoStreamInfo != null) {
            int height = inputStreamDimensions._getHeightInPixels();
            int width = inputStreamDimensions._getWidthInPixels();
            int targetHeight = inputStreamDimensions._getTargetHeightInPixels();
            int targetWidth = inputStreamDimensions._getTargetWidthInPixels();
            int bitrate = inputVideoStreamInfo._getVideoBitrateKBPS();
            int peakBitrate = inputVideoStreamInfo._getVideoPeakBitrateKBPS();

            if (ctx.getConfig().useVideoResolutionType()) {
                outputStream.downloadDescriptor.videoFormatDescriptor = videoFormatIdentifier.selectVideoFormatDescriptor(height, width);
            } else {
                outputStream.downloadDescriptor.videoFormatDescriptor = videoFormatIdentifier.selectVideoFormatDescriptorOld(encodingProfileId, bitrate, height, width, targetHeight, targetWidth);
            }
            outputStream.streamDataDescriptor.bitrate = bitrate;
            outputStream.streamDataDescriptor.peakBitrate = peakBitrate;

            { // DEBUGGING
                VideoFormatDescriptor selectVideoFormatDescriptorNew = videoFormatIdentifier.selectVideoFormatDescriptor(height, width);
                VideoFormatDescriptor selectVideoFormatDescriptorOld = videoFormatIdentifier.selectVideoFormatDescriptorOld(encodingProfileId, bitrate, height, width, targetHeight, targetWidth);
                if (selectVideoFormatDescriptorOld.id != selectVideoFormatDescriptorNew.id) {
                    ctx.getLogger().warn(TransformerLogTag.VideoFormatMismatch, "VideoFormat mismatch: new={}, old={}, downloadableId={}, encodingProfileId={}, height={}, width={}, targetHeight={}, targetWidth={}",
                            selectVideoFormatDescriptorNew.name,
                            selectVideoFormatDescriptorOld.name,
                            inputStream._getDownloadableId(),
                            encodingProfileId,
                            height,
                            width,
                            targetHeight,
                            targetWidth
                            );
                }
            }

            if(targetHeight != Integer.MIN_VALUE && targetWidth != Integer.MIN_VALUE) {
                outputStream.streamDataDescriptor.targetDimensions = new TargetDimensions();
                outputStream.streamDataDescriptor.targetDimensions.heightInPixels = targetHeight;
                outputStream.streamDataDescriptor.targetDimensions.widthInPixels = targetWidth;
            }

            Integer drmKeyGroup = Integer.valueOf((int)streamProfile._getDrmKeyGroup());
            Object drmKey = drmKeysByGroupId.get(drmKeyGroup);
            if(drmKey != null) {
                ////TODO: Probably get rid of this if/else, then don't need special logic to add DrmKeys to the ObjectMapper
                if(inputNonImageInfo != null && inputNonImageInfo._getDrmInfo() != null) {
                    outputStream.drmData = new StreamDrmData();
                    if(drmKeyGroup.intValue() == PackageDataModule.WMDRMKEY_GROUP) {
                        WmDrmKey wmDrmKey = ((WmDrmKey)drmKey).clone();
                        wmDrmKey.downloadableId = outputStream.downloadableId;
                        outputStream.drmData.wmDrmKey = wmDrmKey;
                    } else {
                        outputStream.drmData.drmKey = (DrmKey) drmKey;
                    }
                } else {
                    ///TODO: Why exclude WmDrmKeys?
                    if(drmKeyGroup.intValue() != PackageDataModule.WMDRMKEY_GROUP)
                        objectMapper.addObject(drmKey);
                }

                DrmInfo drmInfo = drmInfoByGroupId.get(drmKeyGroup);
                if(drmInfo != null) {
                    drmInfoData.downloadableIdToDrmInfoMap.put(outputStream.downloadableId, drmInfo);
                }
            }

        }

        outputStream.downloadDescriptor.encodingProfileId = (int)inputStream._getStreamProfileId();
        if(inputAudioStreamInfo != null) {
            if(inputAudioStreamInfo._getAudioLanguageCode() != null)
                outputStream.downloadDescriptor.audioLanguageBcp47code = new Strings(inputAudioStreamInfo._getAudioLanguageCode()._getValue());
            if(outputStream.streamDataDescriptor.bitrate == Integer.MIN_VALUE) {
                outputStream.streamDataDescriptor.bitrate = inputAudioStreamInfo._getAudioBitrateKBPS();
                outputStream.streamDataDescriptor.peakBitrate = Integer.MIN_VALUE;
            }
        }

        if(outputStream.streamDataDescriptor.bitrate == Integer.MIN_VALUE)
            outputStream.streamDataDescriptor.bitrate = 0;
        if(outputStream.streamDataDescriptor.peakBitrate == Integer.MIN_VALUE)
            outputStream.streamDataDescriptor.peakBitrate = 0;

        if(inputTextStreamInfo != null) {
            if(inputTextStreamInfo._getTextLanguageCode() != null)
                outputStream.downloadDescriptor.textLanguageBcp47code = new Strings(inputTextStreamInfo._getTextLanguageCode()._getValue());
            if(inputTextStreamInfo._getTimedTextType() != null) {
                outputStream.downloadDescriptor.timedTextTypeDescriptor = timedTextTypeDescriptorMap.get(inputTextStreamInfo._getTimedTextType()._getValue());
            }

            if(inputTextStreamInfo._getImageTimedTextMasterIndexLength() != Long.MIN_VALUE) {
                outputStream.additionalData.mostlyConstantData.imageSubtitleIndexByteRange = new ImageSubtitleIndexByteRange();
                outputStream.additionalData.mostlyConstantData.imageSubtitleIndexByteRange.masterIndexOffset = inputTextStreamInfo._getImageTimedTextMasterIndexOffset();
                outputStream.additionalData.mostlyConstantData.imageSubtitleIndexByteRange.masterIndexSize = (int) inputTextStreamInfo._getImageTimedTextMasterIndexLength();
            }
        }

        if(inputStreamImageInfo != null && inputStreamImageInfo._getImageCount() != Integer.MIN_VALUE) {
            outputStream.streamDataDescriptor.imageCount = inputStreamImageInfo._getImageCount();
        }

        return outputStream;

    }

    private List<Strings> getTagsList(String tags) {
        List<Strings> cachedList = tagsLists.get(tags);
        if(cachedList != null)
            return cachedList;

        cachedList = new ArrayList<Strings>();
        for(String tag : tags.split(",")) {
            cachedList.add(new Strings(tag));
        }
        tagsLists.put(tags, cachedList);
        return cachedList;
    }


    private Map<String, AssetTypeDescriptor> getAssetTypeDescriptorMap() {
        Map<String, AssetTypeDescriptor> map = new HashMap<String, AssetTypeDescriptor>();

        map.put("primary", assetTypeDescriptor(1, "primary", "Primary"));
        map.put("assistive", assetTypeDescriptor(2, "assistive", "Assistive"));
        map.put("commentary", assetTypeDescriptor(3, "commentary", "Commentary"));
        map.put("clip", assetTypeDescriptor(4, "clip", "Clip"));

        return map;
    }

    private AssetTypeDescriptor assetTypeDescriptor(int id, String name, String description) {
        AssetTypeDescriptor descriptor = new AssetTypeDescriptor();
        descriptor.id = id;
        descriptor.name = new Strings(name);
        descriptor.description = new Strings(description);
        return descriptor;
    }

    private Map<String, Integer> getDeploymentLabelBitsetOffsetMap() {
        Map<String, Integer> map = new HashMap<String, Integer>();

        map.put("PartiallyDeployedReplacement", Integer.valueOf(0));
        map.put("FutureLabel", Integer.valueOf(1));
        map.put("DeployASAP", Integer.valueOf(2));
        map.put("DeployASAP-ignoreRules", Integer.valueOf(3));
        map.put("DoNotPlay", Integer.valueOf(4));

        return map;
    }

    private Map<String, TimedTextTypeDescriptor> getTimedTextTypeDescriptorMap() {
        Map<String, TimedTextTypeDescriptor> map = new HashMap<String, TimedTextTypeDescriptor>();

        map.put("CC", new TimedTextTypeDescriptor("ClosedCaptions"));
        map.put("SUBS", new TimedTextTypeDescriptor("Subtitles"));
        map.put("FN", new TimedTextTypeDescriptor("Forced"));
        map.put("UNKNOWN", new TimedTextTypeDescriptor("Unknown"));

        return map;
    }

}
