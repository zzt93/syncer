package com.github.zzt93.syncer.common.data;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

/**
 * @author zzt
 */
public class SyncDataGsonFactory {

  public static Gson gson() {
    return new GsonBuilder()
        .registerTypeAdapter(SyncByQuery.class, (InstanceCreator<SyncByQuery>) type -> new SyncByQuery(null))
        .registerTypeAdapter(ESScriptUpdate.class, (InstanceCreator<ESScriptUpdate>) type -> new ESScriptUpdate(null))
        .registerTypeAdapter(DataId.class, (JsonSerializer<DataId>) (src, typeOfSrc, context) -> new JsonPrimitive(src.dataId()))
        .registerTypeAdapter(DataId.class, (JsonDeserializer<DataId>) (json, typeOfT, context) -> DataId.fromString(json.getAsString()))
        .create();

  }

}
