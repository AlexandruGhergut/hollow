package com.netflix.vms.transformer.input.api.gen.oscar;

import com.netflix.hollow.api.custom.HollowObjectTypeAPI;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;

@SuppressWarnings("all")
public class MovieExtensionOverrideTypeAPI extends HollowObjectTypeAPI {

    private final MovieExtensionOverrideDelegateLookupImpl delegateLookupImpl;

    public MovieExtensionOverrideTypeAPI(OscarAPI api, HollowObjectTypeDataAccess typeDataAccess) {
        super(api, typeDataAccess, new String[] {
            "entityType",
            "entityValue",
            "attributeValue",
            "dateCreated",
            "lastUpdated",
            "createdBy",
            "updatedBy"
        });
        this.delegateLookupImpl = new MovieExtensionOverrideDelegateLookupImpl(this);
    }

    public int getEntityTypeOrdinal(int ordinal) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleReferencedOrdinal("MovieExtensionOverride", ordinal, "entityType");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[0]);
    }

    public OverrideEntityTypeTypeAPI getEntityTypeTypeAPI() {
        return getAPI().getOverrideEntityTypeTypeAPI();
    }

    public int getEntityValueOrdinal(int ordinal) {
        if(fieldIndex[1] == -1)
            return missingDataHandler().handleReferencedOrdinal("MovieExtensionOverride", ordinal, "entityValue");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[1]);
    }

    public OverrideEntityValueTypeAPI getEntityValueTypeAPI() {
        return getAPI().getOverrideEntityValueTypeAPI();
    }

    public int getAttributeValueOrdinal(int ordinal) {
        if(fieldIndex[2] == -1)
            return missingDataHandler().handleReferencedOrdinal("MovieExtensionOverride", ordinal, "attributeValue");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[2]);
    }

    public AttributeValueTypeAPI getAttributeValueTypeAPI() {
        return getAPI().getAttributeValueTypeAPI();
    }

    public int getDateCreatedOrdinal(int ordinal) {
        if(fieldIndex[3] == -1)
            return missingDataHandler().handleReferencedOrdinal("MovieExtensionOverride", ordinal, "dateCreated");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[3]);
    }

    public DateTypeAPI getDateCreatedTypeAPI() {
        return getAPI().getDateTypeAPI();
    }

    public int getLastUpdatedOrdinal(int ordinal) {
        if(fieldIndex[4] == -1)
            return missingDataHandler().handleReferencedOrdinal("MovieExtensionOverride", ordinal, "lastUpdated");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[4]);
    }

    public DateTypeAPI getLastUpdatedTypeAPI() {
        return getAPI().getDateTypeAPI();
    }

    public int getCreatedByOrdinal(int ordinal) {
        if(fieldIndex[5] == -1)
            return missingDataHandler().handleReferencedOrdinal("MovieExtensionOverride", ordinal, "createdBy");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[5]);
    }

    public StringTypeAPI getCreatedByTypeAPI() {
        return getAPI().getStringTypeAPI();
    }

    public int getUpdatedByOrdinal(int ordinal) {
        if(fieldIndex[6] == -1)
            return missingDataHandler().handleReferencedOrdinal("MovieExtensionOverride", ordinal, "updatedBy");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[6]);
    }

    public StringTypeAPI getUpdatedByTypeAPI() {
        return getAPI().getStringTypeAPI();
    }

    public MovieExtensionOverrideDelegateLookupImpl getDelegateLookupImpl() {
        return delegateLookupImpl;
    }

    @Override
    public OscarAPI getAPI() {
        return (OscarAPI) api;
    }

}