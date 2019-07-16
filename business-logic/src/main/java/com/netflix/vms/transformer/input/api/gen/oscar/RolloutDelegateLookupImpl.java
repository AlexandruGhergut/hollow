package com.netflix.vms.transformer.input.api.gen.oscar;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class RolloutDelegateLookupImpl extends HollowObjectAbstractDelegate implements RolloutDelegate {

    private final RolloutTypeAPI typeAPI;

    public RolloutDelegateLookupImpl(RolloutTypeAPI typeAPI) {
        this.typeAPI = typeAPI;
    }

    public long getRolloutId(int ordinal) {
        return typeAPI.getRolloutId(ordinal);
    }

    public Long getRolloutIdBoxed(int ordinal) {
        return typeAPI.getRolloutIdBoxed(ordinal);
    }

    public long getMovieId(int ordinal) {
        ordinal = typeAPI.getMovieIdOrdinal(ordinal);
        return ordinal == -1 ? Long.MIN_VALUE : typeAPI.getAPI().getMovieIdTypeAPI().getValue(ordinal);
    }

    public Long getMovieIdBoxed(int ordinal) {
        ordinal = typeAPI.getMovieIdOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getMovieIdTypeAPI().getValueBoxed(ordinal);
    }

    public int getMovieIdOrdinal(int ordinal) {
        return typeAPI.getMovieIdOrdinal(ordinal);
    }

    public String getRolloutName(int ordinal) {
        ordinal = typeAPI.getRolloutNameOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getRolloutNameTypeAPI().getValue(ordinal);
    }

    public boolean isRolloutNameEqual(int ordinal, String testValue) {
        ordinal = typeAPI.getRolloutNameOrdinal(ordinal);
        return ordinal == -1 ? testValue == null : typeAPI.getAPI().getRolloutNameTypeAPI().isValueEqual(ordinal, testValue);
    }

    public int getRolloutNameOrdinal(int ordinal) {
        return typeAPI.getRolloutNameOrdinal(ordinal);
    }

    public String getType(int ordinal) {
        ordinal = typeAPI.getTypeOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getRolloutTypeTypeAPI().get_name(ordinal);
    }

    public boolean isTypeEqual(int ordinal, String testValue) {
        ordinal = typeAPI.getTypeOrdinal(ordinal);
        return ordinal == -1 ? testValue == null : typeAPI.getAPI().getRolloutTypeTypeAPI().is_nameEqual(ordinal, testValue);
    }

    public int getTypeOrdinal(int ordinal) {
        return typeAPI.getTypeOrdinal(ordinal);
    }

    public String getStatus(int ordinal) {
        ordinal = typeAPI.getStatusOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getRolloutStatusTypeAPI().get_name(ordinal);
    }

    public boolean isStatusEqual(int ordinal, String testValue) {
        ordinal = typeAPI.getStatusOrdinal(ordinal);
        return ordinal == -1 ? testValue == null : typeAPI.getAPI().getRolloutStatusTypeAPI().is_nameEqual(ordinal, testValue);
    }

    public int getStatusOrdinal(int ordinal) {
        return typeAPI.getStatusOrdinal(ordinal);
    }

    public int getPhasesOrdinal(int ordinal) {
        return typeAPI.getPhasesOrdinal(ordinal);
    }

    public int getCountriesOrdinal(int ordinal) {
        return typeAPI.getCountriesOrdinal(ordinal);
    }

    public long getDateCreated(int ordinal) {
        ordinal = typeAPI.getDateCreatedOrdinal(ordinal);
        return ordinal == -1 ? Long.MIN_VALUE : typeAPI.getAPI().getDateTypeAPI().getValue(ordinal);
    }

    public Long getDateCreatedBoxed(int ordinal) {
        ordinal = typeAPI.getDateCreatedOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getDateTypeAPI().getValueBoxed(ordinal);
    }

    public int getDateCreatedOrdinal(int ordinal) {
        return typeAPI.getDateCreatedOrdinal(ordinal);
    }

    public long getLastUpdated(int ordinal) {
        ordinal = typeAPI.getLastUpdatedOrdinal(ordinal);
        return ordinal == -1 ? Long.MIN_VALUE : typeAPI.getAPI().getDateTypeAPI().getValue(ordinal);
    }

    public Long getLastUpdatedBoxed(int ordinal) {
        ordinal = typeAPI.getLastUpdatedOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getDateTypeAPI().getValueBoxed(ordinal);
    }

    public int getLastUpdatedOrdinal(int ordinal) {
        return typeAPI.getLastUpdatedOrdinal(ordinal);
    }

    public String getCreatedBy(int ordinal) {
        ordinal = typeAPI.getCreatedByOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getStringTypeAPI().getValue(ordinal);
    }

    public boolean isCreatedByEqual(int ordinal, String testValue) {
        ordinal = typeAPI.getCreatedByOrdinal(ordinal);
        return ordinal == -1 ? testValue == null : typeAPI.getAPI().getStringTypeAPI().isValueEqual(ordinal, testValue);
    }

    public int getCreatedByOrdinal(int ordinal) {
        return typeAPI.getCreatedByOrdinal(ordinal);
    }

    public String getUpdatedBy(int ordinal) {
        ordinal = typeAPI.getUpdatedByOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getStringTypeAPI().getValue(ordinal);
    }

    public boolean isUpdatedByEqual(int ordinal, String testValue) {
        ordinal = typeAPI.getUpdatedByOrdinal(ordinal);
        return ordinal == -1 ? testValue == null : typeAPI.getAPI().getStringTypeAPI().isValueEqual(ordinal, testValue);
    }

    public int getUpdatedByOrdinal(int ordinal) {
        return typeAPI.getUpdatedByOrdinal(ordinal);
    }

    public RolloutTypeAPI getTypeAPI() {
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