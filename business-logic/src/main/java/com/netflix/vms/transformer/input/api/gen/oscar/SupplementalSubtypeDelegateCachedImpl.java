package com.netflix.vms.transformer.input.api.gen.oscar;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.api.custom.HollowTypeAPI;
import com.netflix.hollow.api.objects.delegate.HollowCachedDelegate;

@SuppressWarnings("all")
public class SupplementalSubtypeDelegateCachedImpl extends HollowObjectAbstractDelegate implements HollowCachedDelegate, SupplementalSubtypeDelegate {

    private final String value;
    private SupplementalSubtypeTypeAPI typeAPI;

    public SupplementalSubtypeDelegateCachedImpl(SupplementalSubtypeTypeAPI typeAPI, int ordinal) {
        this.value = typeAPI.getValue(ordinal);
        this.typeAPI = typeAPI;
    }

    public String getValue(int ordinal) {
        return value;
    }

    public boolean isValueEqual(int ordinal, String testValue) {
        if(testValue == null)
            return value == null;
        return testValue.equals(value);
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

    public SupplementalSubtypeTypeAPI getTypeAPI() {
        return typeAPI;
    }

    public void updateTypeAPI(HollowTypeAPI typeAPI) {
        this.typeAPI = (SupplementalSubtypeTypeAPI) typeAPI;
    }

}