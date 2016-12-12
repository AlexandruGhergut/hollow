package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.objects.HollowObject;
import com.netflix.hollow.HollowObjectSchema;

@SuppressWarnings("all")
public class RightsHollow extends HollowObject {

    public RightsHollow(RightsDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public ListOfRightsWindowHollow _getWindows() {
        int refOrdinal = delegate().getWindowsOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getListOfRightsWindowHollow(refOrdinal);
    }

    public ListOfRightsContractHollow _getContracts() {
        int refOrdinal = delegate().getContractsOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getListOfRightsContractHollow(refOrdinal);
    }

    public VMSHollowInputAPI api() {
        return typeApi().getAPI();
    }

    public RightsTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected RightsDelegate delegate() {
        return (RightsDelegate)delegate;
    }

}