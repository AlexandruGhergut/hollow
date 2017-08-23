package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.custom.HollowObjectTypeAPI;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;

@SuppressWarnings("all")
public class VideoArtworkSourceTypeAPI extends HollowObjectTypeAPI {

    private final VideoArtworkSourceDelegateLookupImpl delegateLookupImpl;

    VideoArtworkSourceTypeAPI(VMSHollowInputAPI api, HollowObjectTypeDataAccess typeDataAccess) {
        super(api, typeDataAccess, new String[] {
            "sourceFileId",
            "movieId",
            "isFallback",
            "fallbackSourceFileId",
            "seqNum",
            "ordinalPriority",
            "fileImageType",
            "phaseTags",
            "isSmoky",
            "rolloutExclusive",
            "attributes",
            "locales"
        });
        this.delegateLookupImpl = new VideoArtworkSourceDelegateLookupImpl(this);
    }

    public int getSourceFileIdOrdinal(int ordinal) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleReferencedOrdinal("VideoArtworkSource", ordinal, "sourceFileId");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[0]);
    }

    public StringTypeAPI getSourceFileIdTypeAPI() {
        return getAPI().getStringTypeAPI();
    }

    public long getMovieId(int ordinal) {
        if(fieldIndex[1] == -1)
            return missingDataHandler().handleLong("VideoArtworkSource", ordinal, "movieId");
        return getTypeDataAccess().readLong(ordinal, fieldIndex[1]);
    }

    public Long getMovieIdBoxed(int ordinal) {
        long l;
        if(fieldIndex[1] == -1) {
            l = missingDataHandler().handleLong("VideoArtworkSource", ordinal, "movieId");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[1]);
            l = getTypeDataAccess().readLong(ordinal, fieldIndex[1]);
        }
        if(l == Long.MIN_VALUE)
            return null;
        return Long.valueOf(l);
    }



    public boolean getIsFallback(int ordinal) {
        if(fieldIndex[2] == -1)
            return missingDataHandler().handleBoolean("VideoArtworkSource", ordinal, "isFallback") == Boolean.TRUE;
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[2]) == Boolean.TRUE;
    }

    public Boolean getIsFallbackBoxed(int ordinal) {
        if(fieldIndex[2] == -1)
            return missingDataHandler().handleBoolean("VideoArtworkSource", ordinal, "isFallback");
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[2]);
    }



    public int getFallbackSourceFileIdOrdinal(int ordinal) {
        if(fieldIndex[3] == -1)
            return missingDataHandler().handleReferencedOrdinal("VideoArtworkSource", ordinal, "fallbackSourceFileId");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[3]);
    }

    public StringTypeAPI getFallbackSourceFileIdTypeAPI() {
        return getAPI().getStringTypeAPI();
    }

    public int getSeqNum(int ordinal) {
        if(fieldIndex[4] == -1)
            return missingDataHandler().handleInt("VideoArtworkSource", ordinal, "seqNum");
        return getTypeDataAccess().readInt(ordinal, fieldIndex[4]);
    }

    public Integer getSeqNumBoxed(int ordinal) {
        int i;
        if(fieldIndex[4] == -1) {
            i = missingDataHandler().handleInt("VideoArtworkSource", ordinal, "seqNum");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[4]);
            i = getTypeDataAccess().readInt(ordinal, fieldIndex[4]);
        }
        if(i == Integer.MIN_VALUE)
            return null;
        return Integer.valueOf(i);
    }



    public int getOrdinalPriority(int ordinal) {
        if(fieldIndex[5] == -1)
            return missingDataHandler().handleInt("VideoArtworkSource", ordinal, "ordinalPriority");
        return getTypeDataAccess().readInt(ordinal, fieldIndex[5]);
    }

    public Integer getOrdinalPriorityBoxed(int ordinal) {
        int i;
        if(fieldIndex[5] == -1) {
            i = missingDataHandler().handleInt("VideoArtworkSource", ordinal, "ordinalPriority");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[5]);
            i = getTypeDataAccess().readInt(ordinal, fieldIndex[5]);
        }
        if(i == Integer.MIN_VALUE)
            return null;
        return Integer.valueOf(i);
    }



    public int getFileImageTypeOrdinal(int ordinal) {
        if(fieldIndex[6] == -1)
            return missingDataHandler().handleReferencedOrdinal("VideoArtworkSource", ordinal, "fileImageType");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[6]);
    }

    public StringTypeAPI getFileImageTypeTypeAPI() {
        return getAPI().getStringTypeAPI();
    }

    public int getPhaseTagsOrdinal(int ordinal) {
        if(fieldIndex[7] == -1)
            return missingDataHandler().handleReferencedOrdinal("VideoArtworkSource", ordinal, "phaseTags");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[7]);
    }

    public PhaseTagListTypeAPI getPhaseTagsTypeAPI() {
        return getAPI().getPhaseTagListTypeAPI();
    }

    public boolean getIsSmoky(int ordinal) {
        if(fieldIndex[8] == -1)
            return missingDataHandler().handleBoolean("VideoArtworkSource", ordinal, "isSmoky") == Boolean.TRUE;
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[8]) == Boolean.TRUE;
    }

    public Boolean getIsSmokyBoxed(int ordinal) {
        if(fieldIndex[8] == -1)
            return missingDataHandler().handleBoolean("VideoArtworkSource", ordinal, "isSmoky");
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[8]);
    }



    public boolean getRolloutExclusive(int ordinal) {
        if(fieldIndex[9] == -1)
            return missingDataHandler().handleBoolean("VideoArtworkSource", ordinal, "rolloutExclusive") == Boolean.TRUE;
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[9]) == Boolean.TRUE;
    }

    public Boolean getRolloutExclusiveBoxed(int ordinal) {
        if(fieldIndex[9] == -1)
            return missingDataHandler().handleBoolean("VideoArtworkSource", ordinal, "rolloutExclusive");
        return getTypeDataAccess().readBoolean(ordinal, fieldIndex[9]);
    }



    public int getAttributesOrdinal(int ordinal) {
        if(fieldIndex[10] == -1)
            return missingDataHandler().handleReferencedOrdinal("VideoArtworkSource", ordinal, "attributes");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[10]);
    }

    public ArtworkAttributesTypeAPI getAttributesTypeAPI() {
        return getAPI().getArtworkAttributesTypeAPI();
    }

    public int getLocalesOrdinal(int ordinal) {
        if(fieldIndex[11] == -1)
            return missingDataHandler().handleReferencedOrdinal("VideoArtworkSource", ordinal, "locales");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[11]);
    }

    public ArtworkLocaleListTypeAPI getLocalesTypeAPI() {
        return getAPI().getArtworkLocaleListTypeAPI();
    }

    public VideoArtworkSourceDelegateLookupImpl getDelegateLookupImpl() {
        return delegateLookupImpl;
    }

    @Override
    public VMSHollowInputAPI getAPI() {
        return (VMSHollowInputAPI) api;
    }

}