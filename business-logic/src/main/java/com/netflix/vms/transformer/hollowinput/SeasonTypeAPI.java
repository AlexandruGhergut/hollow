package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.custom.HollowObjectTypeAPI;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;

@SuppressWarnings("all")
public class SeasonTypeAPI extends HollowObjectTypeAPI {

    private final SeasonDelegateLookupImpl delegateLookupImpl;

    public SeasonTypeAPI(VMSHollowInputAPI api, HollowObjectTypeDataAccess typeDataAccess) {
        super(api, typeDataAccess, new String[] {
            "sequenceNumber",
            "movieId",
            "episodes",
            "hideEpisodeNumbers",
            "episodicNewBadge",
            "episodeSkipping",
            "filterUnavailableEpisodes",
            "useLatestEpisodeAsDefault",
            "merchOrder"
        });
        this.delegateLookupImpl = new SeasonDelegateLookupImpl(this);
    }

    public long getSequenceNumber(int ordinal) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleLong("Season", ordinal, "sequenceNumber");
        return getTypeDataAccess().readLong(ordinal, fieldIndex[0]);
    }

    public Long getSequenceNumberBoxed(int ordinal) {
        long l;
        if(fieldIndex[0] == -1) {
            l = missingDataHandler().handleLong("Season", ordinal, "sequenceNumber");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[0]);
            l = getTypeDataAccess().readLong(ordinal, fieldIndex[0]);
        }
        if(l == Long.MIN_VALUE)
            return null;
        return Long.valueOf(l);
    }



    public long getMovieId(int ordinal) {
        if(fieldIndex[1] == -1)
            return missingDataHandler().handleLong("Season", ordinal, "movieId");
        return getTypeDataAccess().readLong(ordinal, fieldIndex[1]);
    }

    public Long getMovieIdBoxed(int ordinal) {
        long l;
        if(fieldIndex[1] == -1) {
            l = missingDataHandler().handleLong("Season", ordinal, "movieId");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[1]);
            l = getTypeDataAccess().readLong(ordinal, fieldIndex[1]);
        }
        if(l == Long.MIN_VALUE)
            return null;
        return Long.valueOf(l);
    }



    public int getEpisodesOrdinal(int ordinal) {
        if(fieldIndex[2] == -1)
            return missingDataHandler().handleReferencedOrdinal("Season", ordinal, "episodes");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[2]);
    }

    public EpisodeListTypeAPI getEpisodesTypeAPI() {
        return getAPI().getEpisodeListTypeAPI();
    }

    public boolean getHideEpisodeNumbers(int ordinal) {
        if(fieldIndex[3] == -1)
            return missingDataHandler().handleBoolean("Season", ordinal, "hideEpisodeNumbers") == Boolean.TRUE;
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[3]) == Boolean.TRUE;
    }

    public Boolean getHideEpisodeNumbersBoxed(int ordinal) {
        if(fieldIndex[3] == -1)
            return missingDataHandler().handleBoolean("Season", ordinal, "hideEpisodeNumbers");
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[3]);
    }



    public boolean getEpisodicNewBadge(int ordinal) {
        if(fieldIndex[4] == -1)
            return missingDataHandler().handleBoolean("Season", ordinal, "episodicNewBadge") == Boolean.TRUE;
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[4]) == Boolean.TRUE;
    }

    public Boolean getEpisodicNewBadgeBoxed(int ordinal) {
        if(fieldIndex[4] == -1)
            return missingDataHandler().handleBoolean("Season", ordinal, "episodicNewBadge");
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[4]);
    }



    public int getEpisodeSkipping(int ordinal) {
        if(fieldIndex[5] == -1)
            return missingDataHandler().handleInt("Season", ordinal, "episodeSkipping");
        return getTypeDataAccess().readInt(ordinal, fieldIndex[5]);
    }

    public Integer getEpisodeSkippingBoxed(int ordinal) {
        int i;
        if(fieldIndex[5] == -1) {
            i = missingDataHandler().handleInt("Season", ordinal, "episodeSkipping");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[5]);
            i = getTypeDataAccess().readInt(ordinal, fieldIndex[5]);
        }
        if(i == Integer.MIN_VALUE)
            return null;
        return Integer.valueOf(i);
    }



    public boolean getFilterUnavailableEpisodes(int ordinal) {
        if(fieldIndex[6] == -1)
            return missingDataHandler().handleBoolean("Season", ordinal, "filterUnavailableEpisodes") == Boolean.TRUE;
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[6]) == Boolean.TRUE;
    }

    public Boolean getFilterUnavailableEpisodesBoxed(int ordinal) {
        if(fieldIndex[6] == -1)
            return missingDataHandler().handleBoolean("Season", ordinal, "filterUnavailableEpisodes");
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[6]);
    }



    public boolean getUseLatestEpisodeAsDefault(int ordinal) {
        if(fieldIndex[7] == -1)
            return missingDataHandler().handleBoolean("Season", ordinal, "useLatestEpisodeAsDefault") == Boolean.TRUE;
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[7]) == Boolean.TRUE;
    }

    public Boolean getUseLatestEpisodeAsDefaultBoxed(int ordinal) {
        if(fieldIndex[7] == -1)
            return missingDataHandler().handleBoolean("Season", ordinal, "useLatestEpisodeAsDefault");
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[7]);
    }



    public int getMerchOrderOrdinal(int ordinal) {
        if(fieldIndex[8] == -1)
            return missingDataHandler().handleReferencedOrdinal("Season", ordinal, "merchOrder");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[8]);
    }

    public StringTypeAPI getMerchOrderTypeAPI() {
        return getAPI().getStringTypeAPI();
    }

    public SeasonDelegateLookupImpl getDelegateLookupImpl() {
        return delegateLookupImpl;
    }

    @Override
    public VMSHollowInputAPI getAPI() {
        return (VMSHollowInputAPI) api;
    }

}