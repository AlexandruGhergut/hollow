package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class FlagsDelegateLookupImpl extends HollowObjectAbstractDelegate implements FlagsDelegate {

    private final FlagsTypeAPI typeAPI;

    public FlagsDelegateLookupImpl(FlagsTypeAPI typeAPI) {
        this.typeAPI = typeAPI;
    }

    public boolean getSearchOnly(int ordinal) {
        return typeAPI.getSearchOnly(ordinal);
    }

    public Boolean getSearchOnlyBoxed(int ordinal) {
        return typeAPI.getSearchOnlyBoxed(ordinal);
    }

    public boolean getLocalText(int ordinal) {
        return typeAPI.getLocalText(ordinal);
    }

    public Boolean getLocalTextBoxed(int ordinal) {
        return typeAPI.getLocalTextBoxed(ordinal);
    }

    public boolean getLanguageOverride(int ordinal) {
        return typeAPI.getLanguageOverride(ordinal);
    }

    public Boolean getLanguageOverrideBoxed(int ordinal) {
        return typeAPI.getLanguageOverrideBoxed(ordinal);
    }

    public boolean getLocalAudio(int ordinal) {
        return typeAPI.getLocalAudio(ordinal);
    }

    public Boolean getLocalAudioBoxed(int ordinal) {
        return typeAPI.getLocalAudioBoxed(ordinal);
    }

    public boolean getGoLive(int ordinal) {
        return typeAPI.getGoLive(ordinal);
    }

    public Boolean getGoLiveBoxed(int ordinal) {
        return typeAPI.getGoLiveBoxed(ordinal);
    }

    public boolean getAutoPlay(int ordinal) {
        return typeAPI.getAutoPlay(ordinal);
    }

    public Boolean getAutoPlayBoxed(int ordinal) {
        return typeAPI.getAutoPlayBoxed(ordinal);
    }

    public int getFirstDisplayDateOrdinal(int ordinal) {
        return typeAPI.getFirstDisplayDateOrdinal(ordinal);
    }

    public int getFirstDisplayDatesOrdinal(int ordinal) {
        return typeAPI.getFirstDisplayDatesOrdinal(ordinal);
    }

    public boolean getLiveOnSite(int ordinal) {
        return typeAPI.getLiveOnSite(ordinal);
    }

    public Boolean getLiveOnSiteBoxed(int ordinal) {
        return typeAPI.getLiveOnSiteBoxed(ordinal);
    }

    public int getOffsiteReasonsOrdinal(int ordinal) {
        return typeAPI.getOffsiteReasonsOrdinal(ordinal);
    }

    public boolean getContentApproved(int ordinal) {
        return typeAPI.getContentApproved(ordinal);
    }

    public Boolean getContentApprovedBoxed(int ordinal) {
        return typeAPI.getContentApprovedBoxed(ordinal);
    }

    public boolean getAllowIncomplete(int ordinal) {
        return typeAPI.getAllowIncomplete(ordinal);
    }

    public Boolean getAllowIncompleteBoxed(int ordinal) {
        return typeAPI.getAllowIncompleteBoxed(ordinal);
    }

    public boolean getGoLivePartialSubDubIgnored(int ordinal) {
        return typeAPI.getGoLivePartialSubDubIgnored(ordinal);
    }

    public Boolean getGoLivePartialSubDubIgnoredBoxed(int ordinal) {
        return typeAPI.getGoLivePartialSubDubIgnoredBoxed(ordinal);
    }

    public int getAlternateLanguageOrdinal(int ordinal) {
        return typeAPI.getAlternateLanguageOrdinal(ordinal);
    }

    public boolean getHasRequiredLanguages(int ordinal) {
        return typeAPI.getHasRequiredLanguages(ordinal);
    }

    public Boolean getHasRequiredLanguagesBoxed(int ordinal) {
        return typeAPI.getHasRequiredLanguagesBoxed(ordinal);
    }

    public boolean getHasRequiredStreams(int ordinal) {
        return typeAPI.getHasRequiredStreams(ordinal);
    }

    public Boolean getHasRequiredStreamsBoxed(int ordinal) {
        return typeAPI.getHasRequiredStreamsBoxed(ordinal);
    }

    public boolean getReleaseAsAvailable(int ordinal) {
        return typeAPI.getReleaseAsAvailable(ordinal);
    }

    public Boolean getReleaseAsAvailableBoxed(int ordinal) {
        return typeAPI.getReleaseAsAvailableBoxed(ordinal);
    }

    public int getRemoveAssetOrdinal(int ordinal) {
        return typeAPI.getRemoveAssetOrdinal(ordinal);
    }

    public boolean getRemoveFromWebsiteOverride(int ordinal) {
        return typeAPI.getRemoveFromWebsiteOverride(ordinal);
    }

    public Boolean getRemoveFromWebsiteOverrideBoxed(int ordinal) {
        return typeAPI.getRemoveFromWebsiteOverrideBoxed(ordinal);
    }

    public int getRequiredLangsOrdinal(int ordinal) {
        return typeAPI.getRequiredLangsOrdinal(ordinal);
    }

    public boolean getSearchOnlyOverride(int ordinal) {
        return typeAPI.getSearchOnlyOverride(ordinal);
    }

    public Boolean getSearchOnlyOverrideBoxed(int ordinal) {
        return typeAPI.getSearchOnlyOverrideBoxed(ordinal);
    }

    public FlagsTypeAPI getTypeAPI() {
        return typeAPI;
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

}