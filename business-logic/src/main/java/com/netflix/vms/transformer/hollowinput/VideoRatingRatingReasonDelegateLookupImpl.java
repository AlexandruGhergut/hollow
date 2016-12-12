package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.HollowObjectSchema;

@SuppressWarnings("all")
public class VideoRatingRatingReasonDelegateLookupImpl extends HollowObjectAbstractDelegate implements VideoRatingRatingReasonDelegate {

    private final VideoRatingRatingReasonTypeAPI typeAPI;

    public VideoRatingRatingReasonDelegateLookupImpl(VideoRatingRatingReasonTypeAPI typeAPI) {
        this.typeAPI = typeAPI;
    }

    public boolean getOrdered(int ordinal) {
        return typeAPI.getOrdered(ordinal);
    }

    public Boolean getOrderedBoxed(int ordinal) {
        return typeAPI.getOrderedBoxed(ordinal);
    }

    public boolean getImageOnly(int ordinal) {
        return typeAPI.getImageOnly(ordinal);
    }

    public Boolean getImageOnlyBoxed(int ordinal) {
        return typeAPI.getImageOnlyBoxed(ordinal);
    }

    public int getIdsOrdinal(int ordinal) {
        return typeAPI.getIdsOrdinal(ordinal);
    }

    public VideoRatingRatingReasonTypeAPI getTypeAPI() {
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