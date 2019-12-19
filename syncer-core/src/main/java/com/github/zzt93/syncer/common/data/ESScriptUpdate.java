package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.github.zzt93.syncer.common.util.EsTypeUtil.convertType;

/**
 * @see ExtraQuery
 * @see SyncByQuery
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html#_scripted_updates
 */
public class ESScriptUpdate implements Serializable, com.github.zzt93.syncer.data.ESScriptUpdate {

  private static final Logger logger = LoggerFactory.getLogger(ESScriptUpdate.class);
  private static final ArrayList<Object> NEW = new ArrayList<>(0);
  private static final String BY_ID_SUFFIX = "_id";

  // todo other script op: +=, contains
  private final HashMap<String, Object> mergeToList = new HashMap<>();
  private final HashMap<String, FieldAndId> mergeToListById = new HashMap<>();
  private final HashMap<String, HashMap<String, Object>> nested = new HashMap<>();

  private final transient SyncData outer;
  private SimpleEventType oldType;

  ESScriptUpdate(SyncData data) {
    outer = data;
  }

  public static void makeScript(StringBuilder code, String op, String endOp, HashMap<String, Object> data,
                                HashMap<String, Object> params) {
    makeScript(code, "ctx._source.", op, endOp, data, params);
  }

  private static void makeScript(StringBuilder code, String prefix, String op, String endOp, HashMap<String, Object> data,
                         HashMap<String, Object> params) {
    for (String col : data.keySet()) {
      code.append(prefix).append(col).append(op).append(col).append(endOp);
    }
    scriptCheck(code, data, params);
  }

  private static void scriptCheck(StringBuilder code, HashMap<String, Object> data,
                                  HashMap<String, Object> params) {
    int before = params.size();
    params.putAll(data);
    if (before + data.size() != params.size()) {
      throw new InvalidConfigException("Key conflict happens when making script [" + code + "], "
          + "check config file about `syncByQuery()` (Notice the `syncByQuery()` will default use all fields for 'set' update)");
    }
  }

  public ESScriptUpdate mergeToList(String listFieldNameInEs, String toMergeFieldNameInSyncData) {
    Object field = convertType(outer.getField(toMergeFieldNameInSyncData));
    outer.removeField(toMergeFieldNameInSyncData);
    switch (outer.getType()) {
      case DELETE:
      case WRITE:
        mergeToList.put(listFieldNameInEs, field);
        break;
      default:
        logger.warn("Not support update list variable for {}", outer.getType());
    }
    outer.toUpdate();
    return this;
  }

  private void generateFromMergeToList(StringBuilder code, HashMap<String, Object> params) {
//    makeScript(code, ".add(params.", ");", esScriptUpdate.getMergeToList(), params);
//    makeScript(code, ".removeIf(Predicate.isEqual(params.", ");", esScriptUpdate.getRemove(), params);
  }

  public ESScriptUpdate mergeToListById(String listFieldNameInEs, String parentIdName, String toMergeFieldNameInSyncData) {
    Object id = convertType(outer.getId());
    Object field = convertType(outer.getField(toMergeFieldNameInSyncData));
    outer.removeField(toMergeFieldNameInSyncData);
    oldType = outer.getType();
    switch (oldType) {
      case WRITE:
      case DELETE:
        mergeToListById.put(listFieldNameInEs, new FieldAndId(id, field));
        break;
      case UPDATE:
        mergeToListById.put(listFieldNameInEs, new FieldAndId(id, field).setBeforeItem(convertType(outer.getBefore(toMergeFieldNameInSyncData))));
        break;
      default:
        throw new UnsupportedOperationException();
    }
    outer.setId(outer.getField(parentIdName))
        .removeField(parentIdName)
        .toUpdate();
    return this;
  }

  /**
   * https://www.elastic.co/guide/en/elasticsearch/reference/5.4/painless-api-reference.html
   *
   * To avoid losing data, the update API retrieves the current _version of the document in the retrieve step,
   * and passes that to the index request during the reindex step.
   * If another process has changed the document between retrieve and reindex,
   * then the _version number won’t match and the update request will fail.
   */
  private void generateFromMergeToListById(StringBuilder code, HashMap<String, Object> params) {
    HashMap<String, Object> tmp = new HashMap<>(mergeToListById.size() * 3);
    switch (oldType) {
      case DELETE:
        mergeToListById.forEach((k, v) -> {
          code.append(String.format(
              "if (ctx._source.%s_id.removeIf(Predicate.isEqual(params.%s_id))) {" +
                  "ctx._source.%s.removeIf(Predicate.isEqual(params.%s)); " +
                  "}", k, k, k, k));
          tmp.put(k + BY_ID_SUFFIX, v.getId());
          tmp.put(k, v.getNewItem());
        });
        break;
      case WRITE:
        mergeToListById.forEach((k, v) -> {
          code.append(String.format(
              "if (!ctx._source.%s_id.contains(params.%s_id)) {" +
                  "ctx._source.%s_id.add(params.%s_id); ctx._source.%s.add(params.%s); " +
                  "}", k, k, k, k, k, k));
          tmp.put(k + BY_ID_SUFFIX, v.getId());
          tmp.put(k, v.getNewItem());
        });
        break;
      case UPDATE:
        mergeToListById.forEach((k, v) -> {
          code.append(String.format(
              "if (ctx._source.%s_id.contains(params.%s_id)) {" +
                  "ctx._source.%s.set(ctx._source.%s.indexOf(params.%s_before), params.%s); " +
                  "}", k, k, k, k, k, k));
          tmp.put(k + BY_ID_SUFFIX, v.getId());
          tmp.put(k + "_before", v.getBeforeItem());
          tmp.put(k, v.getNewItem());
        });
        break;
      default:
        throw new UnsupportedOperationException();
    }
    scriptCheck(code, tmp, params);
  }

  @Override
  public ESScriptUpdate mergeToNestedById(String listFieldNameInEs, String parentIdName, String... toMergeFieldsNameInSyncData) {
    Object id = convertType(outer.getId());
    HashMap<String, Object> obj = Maps.newHashMap();
    obj.put("id", id);
    oldType = outer.getType();
    switch (oldType) {
      case DELETE:
        nested.put(listFieldNameInEs, obj);
        break;
      case WRITE:
      case UPDATE:
        for (String s : toMergeFieldsNameInSyncData) {
          Object field = convertType(outer.getField(s));
          obj.put(s, field);
        }
        nested.put(listFieldNameInEs, obj);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    outer.setId(outer.getField(parentIdName))
        .removeField(parentIdName)
        .removeFields(toMergeFieldsNameInSyncData)
        .toUpdate();
    return this;
  }

  private void generateFromMergeToNestedById(StringBuilder code, HashMap<String, Object> params) {
    HashMap<String, Object> subParam = new HashMap<>(nested.size() * 3);
    switch (oldType) {
      case DELETE:
        nested.forEach((k, v) -> {
          code.append(String.format(
              "ctx._source.%s.removeIf(e -> e.id.equals(params.%s_id)); ", k, k));
          subParam.put(k + BY_ID_SUFFIX, v.get("id"));
        });
        break;
      case WRITE:
        nested.forEach((k, v) -> {
          code.append(String.format(
              "if (ctx._source.%s.find(e -> e.id.equals(params.%s_id)) == null) {" +
              "  ctx._source.%s.add(params.%s);" +
              "}", k, k, k, k));
          subParam.put(k + BY_ID_SUFFIX, v.get("id"));
          subParam.put(k, v);
        });
        break;
      case UPDATE:
        nested.forEach((k, v) -> {
          StringBuilder setCode = new StringBuilder();
          makeScript(setCode, "target.", " = params.", ";", v, params);
          code.append(String.format(
              "def target = ctx._source.%s.find(e -> e.id.equals(params.%s_id));" +
              "if (target != null) {" +
              " " + setCode.toString() +
              "}", k, k));
          subParam.put(k + BY_ID_SUFFIX, v.get("id"));
        });
        break;
      default:
        throw new UnsupportedOperationException();
    }
    scriptCheck(code, subParam, params);
  }


  public boolean needScript() {
    return !mergeToList.isEmpty() || !mergeToListById.isEmpty() || !nested.isEmpty();
  }

  @Override
  public String toString() {
    return "SyncByQueryES{" +
        "mergeToListById=" + mergeToListById +
        ", nested=" + nested +
        ", outer=SyncData@" + Integer.toHexString(outer.hashCode()) +
        '}';
  }

  public void upsert(HashMap<String, Object> upsert) {
    switch (oldType) {
      case WRITE:
      case DELETE:
        for (String col : mergeToListById.keySet()) {
          upsert.put(col + BY_ID_SUFFIX, NEW);
          upsert.put(col, NEW);
        }
        for (String s : nested.keySet()) {
          upsert.put(s, NEW);
        }
        break;
      case UPDATE:
        for (Map.Entry<String, FieldAndId> entry : mergeToListById.entrySet()) {
          upsert.put(entry.getKey() + BY_ID_SUFFIX, Lists.newArrayList(entry.getValue().getId()));
          upsert.put(entry.getKey(), Lists.newArrayList(entry.getValue().getNewItem()));
        }
        for (Map.Entry<String, HashMap<String, Object>> entry : nested.entrySet()) {
          upsert.put(entry.getKey(), Lists.newArrayList(entry.getValue()));
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }

  }

  public void generateScript(StringBuilder code, HashMap<String, Object> params) {
    generateFromMergeToList(code, params);
    generateFromMergeToListById(code, params);
    generateFromMergeToNestedById(code, params);
  }

  public static class FieldAndId {
    private final Object id;
    private final Object newItem;
    private Object beforeItem;

    FieldAndId(Object id, Object newItem) {
      this.id = id;
      this.newItem = newItem;
    }

    public Object getId() {
      return id;
    }

    Object getNewItem() {
      return newItem;
    }

    Object getBeforeItem() {
      return beforeItem;
    }

    FieldAndId setBeforeItem(Object beforeItem) {
      this.beforeItem = beforeItem;
      return this;
    }
  }
}
