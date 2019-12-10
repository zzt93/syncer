package com.github.zzt93.syncer.common.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

import static com.github.zzt93.syncer.common.util.EsTypeUtil.convertType;

/**
 * @see ExtraQuery
 * @see SyncByQuery
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html#_scripted_updates
 */
public class ESScriptUpdate implements Serializable, com.github.zzt93.syncer.data.ESScriptUpdate {

  private static final Logger logger = LoggerFactory.getLogger(ESScriptUpdate.class);

  // todo other script op: +=, contains
  private final HashMap<String, Object> append = new HashMap<>();
  private final HashMap<String, NestedObjWithId> objAppend = new HashMap<>();
  private final HashMap<String, Object> remove = new HashMap<>();
  private final HashMap<String, NestedObjWithId> objRemove = new HashMap<>();
  private final HashMap<String, NestedObjWithId> objUpdate = new HashMap<>();
  private final transient SyncData outer;

  public static class NestedObjWithId {
    private final Object id;
    private final Object newItem;
    private Object beforeItem;

    NestedObjWithId(Object id, Object newItem) {
      this.id = id;
      this.newItem = newItem;
    }

    public Object getId() {
      return id;
    }

    public Object getNewItem() {
      return newItem;
    }

    public Object getBeforeItem() {
      return beforeItem;
    }

    NestedObjWithId setBeforeItem(Object beforeItem) {
      this.beforeItem = beforeItem;
      return this;
    }
  }

  ESScriptUpdate(SyncData data) {
    outer = data;
  }

  public ESScriptUpdate mergeToList(String listFieldNameInEs, String syncDataFieldName) {
    Object field = convertType(outer.getField(syncDataFieldName));
    outer.removeField(syncDataFieldName);
    switch (outer.getType()) {
      case DELETE:
        remove.put(listFieldNameInEs, field);
        break;
      case WRITE:
        append.put(listFieldNameInEs, field);
        break;
      default:
        logger.warn("Not support update list variable for {}", outer.getType());
    }
    outer.toUpdate();
    return this;
  }

  public ESScriptUpdate mergeToListById(String listFieldNameInEs, String syncDataFieldName) {
    Object id = convertType(outer.getId());
    Object field = convertType(outer.getField(syncDataFieldName));
    outer.removeField(syncDataFieldName);
    switch (outer.getType()) {
      case DELETE:
        objRemove.put(listFieldNameInEs, new NestedObjWithId(id, field));
        break;
      case WRITE:
        objAppend.put(listFieldNameInEs, new NestedObjWithId(id, field));
        break;
      case UPDATE:
        objUpdate.put(listFieldNameInEs, new NestedObjWithId(id, field).setBeforeItem(convertType(outer.getBefore(syncDataFieldName))));
        break;
      default:
        throw new UnsupportedOperationException();
    }
    outer.toUpdate();
    return this;
  }

  public boolean needScript() {
    return !append.isEmpty() || !remove.isEmpty()
        || !objUpdate.isEmpty() || !objAppend.isEmpty() || !objRemove.isEmpty() ;
  }

  public HashMap<String, Object> getAppend() {
    return append;
  }

  public HashMap<String, Object> getRemove() {
    return remove;
  }

  public HashMap<String, NestedObjWithId> getObjAppend() {
    return objAppend;
  }

  public HashMap<String, NestedObjWithId> getObjRemove() {
    return objRemove;
  }

  public HashMap<String, NestedObjWithId> getObjUpdate() {
    return objUpdate;
  }

  @Override
  public String toString() {
    return "SyncByQueryES{" +
        ", append=" + append +
        ", remove=" + remove +
        ", outer=SyncData@" + Integer.toHexString(outer.hashCode()) +
        '}';
  }
}
