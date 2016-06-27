package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.HollowObjectSchema;
import com.netflix.hollow.read.customapi.HollowTypeAPI;
import com.netflix.hollow.objects.delegate.HollowCachedDelegate;

@SuppressWarnings("all")
public class ISOCountryDelegateCachedImpl extends HollowObjectAbstractDelegate implements HollowCachedDelegate, ISOCountryDelegate {

    private final String value;
   private ISOCountryTypeAPI typeAPI;

    public ISOCountryDelegateCachedImpl(ISOCountryTypeAPI typeAPI, int ordinal) {
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

    public ISOCountryTypeAPI getTypeAPI() {
        return typeAPI;
    }

    public void updateTypeAPI(HollowTypeAPI typeAPI) {
        this.typeAPI = (ISOCountryTypeAPI) typeAPI;
    }

}