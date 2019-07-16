package com.netflix.vms.transformer.input.api.gen.oscar;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.data.AbstractHollowDataAccessor;
import com.netflix.hollow.core.index.key.PrimaryKey;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;

@SuppressWarnings("all")
public class ImageTypeDataAccessor extends AbstractHollowDataAccessor<ImageType> {

    public static final String TYPE = "ImageType";
    private OscarAPI api;

    public ImageTypeDataAccessor(HollowConsumer consumer) {
        super(consumer, TYPE);
        this.api = (OscarAPI)consumer.getAPI();
    }

    public ImageTypeDataAccessor(HollowReadStateEngine rStateEngine, OscarAPI api) {
        super(rStateEngine, TYPE);
        this.api = api;
    }

    public ImageTypeDataAccessor(HollowReadStateEngine rStateEngine, OscarAPI api, String ... fieldPaths) {
        super(rStateEngine, TYPE, fieldPaths);
        this.api = api;
    }

    public ImageTypeDataAccessor(HollowReadStateEngine rStateEngine, OscarAPI api, PrimaryKey primaryKey) {
        super(rStateEngine, TYPE, primaryKey);
        this.api = api;
    }

    @Override public ImageType getRecord(int ordinal){
        return api.getImageType(ordinal);
    }

}