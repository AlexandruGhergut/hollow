package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.api.custom.HollowTypeAPI;
import com.netflix.hollow.api.objects.delegate.HollowCachedDelegate;

@SuppressWarnings("all")
public class VideoAwardMappingDelegateCachedImpl extends HollowObjectAbstractDelegate implements HollowCachedDelegate, VideoAwardMappingDelegate {

    private final Long awardId;
    private final Long sequenceNumber;
    private final Long year;
    private final Long personId;
    private final Boolean winner;
    private VideoAwardMappingTypeAPI typeAPI;

    public VideoAwardMappingDelegateCachedImpl(VideoAwardMappingTypeAPI typeAPI, int ordinal) {
        this.awardId = typeAPI.getAwardIdBoxed(ordinal);
        this.sequenceNumber = typeAPI.getSequenceNumberBoxed(ordinal);
        this.year = typeAPI.getYearBoxed(ordinal);
        this.personId = typeAPI.getPersonIdBoxed(ordinal);
        this.winner = typeAPI.getWinnerBoxed(ordinal);
        this.typeAPI = typeAPI;
    }

    public long getAwardId(int ordinal) {
        if(awardId == null)
            return Long.MIN_VALUE;
        return awardId.longValue();
    }

    public Long getAwardIdBoxed(int ordinal) {
        return awardId;
    }

    public long getSequenceNumber(int ordinal) {
        if(sequenceNumber == null)
            return Long.MIN_VALUE;
        return sequenceNumber.longValue();
    }

    public Long getSequenceNumberBoxed(int ordinal) {
        return sequenceNumber;
    }

    public long getYear(int ordinal) {
        if(year == null)
            return Long.MIN_VALUE;
        return year.longValue();
    }

    public Long getYearBoxed(int ordinal) {
        return year;
    }

    public long getPersonId(int ordinal) {
        if(personId == null)
            return Long.MIN_VALUE;
        return personId.longValue();
    }

    public Long getPersonIdBoxed(int ordinal) {
        return personId;
    }

    public boolean getWinner(int ordinal) {
        if(winner == null)
            return false;
        return winner.booleanValue();
    }

    public Boolean getWinnerBoxed(int ordinal) {
        return winner;
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

    public VideoAwardMappingTypeAPI getTypeAPI() {
        return typeAPI;
    }

    public void updateTypeAPI(HollowTypeAPI typeAPI) {
        this.typeAPI = (VideoAwardMappingTypeAPI) typeAPI;
    }

}