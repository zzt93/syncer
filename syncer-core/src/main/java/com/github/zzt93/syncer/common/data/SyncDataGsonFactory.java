package com.github.zzt93.syncer.common.data;

import com.google.gson.*;
import org.springframework.expression.spel.support.StandardEvaluationContext;

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

  public static void afterRecover(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    context.setTypeLocator(new CommonTypeLocator());
    context.setRootObject(data);
  }
}
