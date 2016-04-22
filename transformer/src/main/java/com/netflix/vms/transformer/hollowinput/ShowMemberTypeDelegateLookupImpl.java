package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.HollowObjectSchema;

public class ShowMemberTypeDelegateLookupImpl extends HollowObjectAbstractDelegate implements ShowMemberTypeDelegate {

    private final ShowMemberTypeTypeAPI typeAPI;

    public ShowMemberTypeDelegateLookupImpl(ShowMemberTypeTypeAPI typeAPI) {
        this.typeAPI = typeAPI;
    }

    public int getCountryCodesOrdinal(int ordinal) {
        return typeAPI.getCountryCodesOrdinal(ordinal);
    }

    public long getSequenceLabelId(int ordinal) {
        return typeAPI.getSequenceLabelId(ordinal);
    }

    public Long getSequenceLabelIdBoxed(int ordinal) {
        return typeAPI.getSequenceLabelIdBoxed(ordinal);
    }

    public ShowMemberTypeTypeAPI getTypeAPI() {
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