package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.objects.HollowList;
import com.netflix.hollow.core.schema.HollowListSchema;
import com.netflix.hollow.api.objects.delegate.HollowListDelegate;
import com.netflix.hollow.api.objects.generic.GenericHollowRecordHelper;

@SuppressWarnings("all")
public class VideoGeneralAliasListHollow extends HollowList<VideoGeneralAliasHollow> {

    public VideoGeneralAliasListHollow(HollowListDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    @Override
    public VideoGeneralAliasHollow instantiateElement(int ordinal) {
        return (VideoGeneralAliasHollow) api().getVideoGeneralAliasHollow(ordinal);
    }

    @Override
    public boolean equalsElement(int elementOrdinal, Object testObject) {
        return GenericHollowRecordHelper.equalObject(getSchema().getElementType(), elementOrdinal, testObject);
    }

    public VMSHollowInputAPI api() {
        return typeApi().getAPI();
    }

    public VideoGeneralAliasListTypeAPI typeApi() {
        return (VideoGeneralAliasListTypeAPI) delegate.getTypeAPI();
    }

}