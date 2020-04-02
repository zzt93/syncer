package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.consumer.output.channel.mapper.KVMapper;
import com.github.zzt93.syncer.data.Filter;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.github.zzt93.syncer.common.util.EsTypeUtil.scriptConvert;

/**
 * @see ExtraQuery
 * @see SyncByQuery
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html#_scripted_updates
 */
public class ESScriptUpdate implements Serializable, com.github.zzt93.syncer.data.ESScriptUpdate {

  private static final Logger logger = LoggerFactory.getLogger(ESScriptUpdate.class);
  private static final ArrayList<Object> NEW = new ArrayList<>(0);
  public static final String BY_ID_SUFFIX = "_id";
  private static final String CHILD_FILTER_KEY = "";
  private static final String CHILD_DOC_ID = "id";

  // todo other script op: +=, contains
  private final HashMap<String, Object> mergeToList = new HashMap<>();
  private final HashMap<String, FieldAndId> mergeToListById = new HashMap<>();
  private final HashMap<String, HashMap<String, Object>> nested = new HashMap<>();

  private final transient SyncData outer;
  private SimpleEventType oldType;
  private Filter parentFilter;
  private String script;
  private Map<String, Object> params;

  ESScriptUpdate(SyncData data) {
    outer = data;
    this.parentFilter = Filter.id();
  }

  ESScriptUpdate(SyncData syncData, Filter parentFilter) {
    outer = syncData;
    this.parentFilter = parentFilter;
  }

  ESScriptUpdate(SyncData syncData, String script, Map<String, Object> params) {
    outer = syncData;
    this.script = script;
    this.params = params;
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
      logger.warn("Key conflict happens when making script [{}] {}", code, params);
    }
  }

  public ESScriptUpdate mergeToList(String listFieldNameInEs, String toMergeFieldName) {
    Object field = scriptConvert(outer.getField(toMergeFieldName));
    outer.removeField(toMergeFieldName);
    oldType = outer.getType();
    switch (outer.getType()) {
      case DELETE:
      case WRITE:
        mergeToList.put(listFieldNameInEs, field);
        break;
      default:
        logger.error("Not support `mergeToList` for UPDATE, use `mergeToListById` or `mergeToNestedById` instead");
        throw new UnsupportedOperationException();
    }
    assert parentFilter.isId();
    if (!parentFilter.fieldUseId()) {
      String parentIdName = parentFilter.getFieldKeyName();
      outer.setId(outer.removeField(parentIdName));
    }
    outer.toUpdate();
    return this;
  }

  private void generateFromMergeToList(StringBuilder code, HashMap<String, Object> params) {
    switch (oldType) {
      case DELETE:
        makeScript(code, ".removeIf(Predicate.isEqual(params.", "));", mergeToList, params);
        break;
      case WRITE:
        makeScript(code, ".add(params.", ");", mergeToList, params);
        break;
      case UPDATE:
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public ESScriptUpdate mergeToListById(String listFieldNameInEs, String toMergeFieldName) {
    Object id = scriptConvert(outer.getId());
    Object field = scriptConvert(outer.getField(toMergeFieldName));
    outer.removeField(toMergeFieldName);
    oldType = outer.getType();
    switch (oldType) {
      case WRITE:
      case DELETE:
        mergeToListById.put(listFieldNameInEs, new FieldAndId(id, field));
        break;
      case UPDATE:
        mergeToListById.put(listFieldNameInEs, new FieldAndId(id, field).setBeforeItem(scriptConvert(outer.getBefore(toMergeFieldName))));
        break;
      default:
        throw new UnsupportedOperationException();
    }
    assert parentFilter.isId();
    if (!parentFilter.fieldUseId()) {
      String parentIdName = parentFilter.getFieldKeyName();
      outer.setId(outer.removeField(parentIdName));
    }
    outer.toUpdate();
    return this;
  }

  /**
   * https://www.elastic.co/guide/en/elasticsearch/reference/5.4/painless-api-reference.html
   * <p>
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
  public ESScriptUpdate mergeToNestedById(String listFieldNameInEs, String... toMergeFieldsName) {
    return mergeToNestedByFilter(listFieldNameInEs, Filter.id(), toMergeFieldsName);
  }

  private ESScriptUpdate mergeToNestedByFilter(String listFieldNameInEs, Filter childFilter, String... toMergeFieldsName) {
    HashMap<String, Object> nestedObj = Maps.newHashMap();
    nestedObj.put(CHILD_FILTER_KEY, childFilter);
    String key = childFilter.getDocKeyNameOrDefault(CHILD_DOC_ID);
    Object value = scriptConvert(childFilter.getFieldValue(outer));
    nestedObj.put(key, value);

    oldType = outer.getType();
    switch (oldType) {
      case DELETE:
        nested.put(listFieldNameInEs, nestedObj);
        break;
      case WRITE:
      case UPDATE:
        for (String s : toMergeFieldsName) {
          Object field = outer.getField(s);
          nestedObj.put(s, field);
        }
        nested.put(listFieldNameInEs, nestedObj);
        break;
      default:
        throw new UnsupportedOperationException();
    }

    if (parentFilter.isId()) {
      if (!parentFilter.fieldUseId()) {
        String parentIdName = parentFilter.getFieldKeyName();
        outer.setId(outer.removeField(parentIdName));
      }
    } else {
      SyncByQuery syncByQuery = outer.syncByQuery();
      do {
        syncByQuery.syncBy(parentFilter.getDocKeyName(), parentFilter.getFieldValue(outer));
        parentFilter = parentFilter.next();
      } while (parentFilter != null);
    }

    outer.removeFields(toMergeFieldsName).toUpdate();
    return this;
  }

  @Override
  public ESScriptUpdate mergeToNestedByQuery(String listFieldNameInEs, Filter childFilter, String... toMergeFieldsName) {
    if (outer.getType() == SimpleEventType.WRITE) {
      logger.error("Not support mergeToNestedByQuery for SimpleEventType.WRITE, ignore invocation");
      return this;
    }
    return mergeToNestedByFilter(listFieldNameInEs, childFilter, toMergeFieldsName);
  }

  private void generateFromMergeToNested(StringBuilder code, HashMap<String, Object> params) {
    HashMap<String, Object> subParam = new HashMap<>(nested.size() * 3);
    switch (oldType) {
      case DELETE:
        nested.forEach((nestedFieldName, nestedObj) -> {
          Filter filter = (Filter) nestedObj.remove(CHILD_FILTER_KEY);
          String docKeyNameOrDefault = filter.getDocKeyNameOrDefault(CHILD_DOC_ID);

          KVMapper.map(nestedObj, nestedObj);

          code.append(String.format(
              "ctx._source.%s.removeIf(e -> e.%s.equals(params.%s)); ", nestedFieldName, docKeyNameOrDefault, docKeyNameOrDefault));
          subParam.put(docKeyNameOrDefault, nestedObj.remove(docKeyNameOrDefault));
        });
        break;
      case WRITE:
        nested.forEach((nestedFieldName, nestedObj) -> {
          Filter filter = (Filter) nestedObj.remove(CHILD_FILTER_KEY);
          String docKeyNameOrDefault = filter.getDocKeyNameOrDefault(CHILD_DOC_ID);

          KVMapper.map(nestedObj, nestedObj);

          code.append(String.format(
              "if (ctx._source.%s.find(e -> e.%s.equals(params.%s)) == null) {" +
                  "  ctx._source.%s.add(params.%s);" +
                  "}", nestedFieldName, docKeyNameOrDefault, docKeyNameOrDefault, nestedFieldName, nestedFieldName));
          subParam.put(docKeyNameOrDefault, nestedObj.get(docKeyNameOrDefault));
          subParam.put(nestedFieldName, nestedObj);
        });
        break;
      case UPDATE:
        nested.forEach((nestedFieldName, nestedObj) -> {
          Filter filter = (Filter) nestedObj.remove(CHILD_FILTER_KEY);
          String docKeyNameOrDefault = filter.getDocKeyNameOrDefault(CHILD_DOC_ID);

          KVMapper.map(nestedObj, nestedObj);

          // remove filterKey from nestedObj because filterKey has no change
          subParam.put(docKeyNameOrDefault, nestedObj.remove(docKeyNameOrDefault));
          StringBuilder setCode = new StringBuilder();
          makeScript(setCode, "target.", " = params.", ";", nestedObj, params);
          code.append(String.format(
              "def target = ctx._source.%s.find(e -> e.%s.equals(params.%s));" +
                  "if (target != null) {" +
                  " " + setCode.toString() +
                  "}", nestedFieldName, docKeyNameOrDefault, docKeyNameOrDefault));
        });
        break;
      default:
        throw new UnsupportedOperationException();
    }
    scriptCheck(code, subParam, params);
  }


  public boolean needScript() {
    return StringUtils.isNotBlank(script) || !mergeToList.isEmpty() || !mergeToListById.isEmpty() || !nested.isEmpty();
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
    if (notSupportUpsert()) {
      return;
    }
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

  public boolean notSupportUpsert() {
    return oldType == null;
  }

  public void generateMergeScript(StringBuilder code, HashMap<String, Object> params) {
    if (script != null && this.params != null) {
      code.append(script);
      KVMapper.map(this.params, this.params);
      for (Map.Entry<String, Object> e : this.params.entrySet()) {
        params.put(e.getKey(), scriptConvert(e.getValue()));
      }
      return;
    }
    generateFromMergeToList(code, params);
    generateFromMergeToListById(code, params);
    generateFromMergeToNested(code, params);
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
