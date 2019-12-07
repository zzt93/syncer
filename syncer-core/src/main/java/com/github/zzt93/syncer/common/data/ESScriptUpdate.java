package com.github.zzt93.syncer.common.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @see ExtraQuery
 * @see SyncByQuery
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html#_scripted_updates
 */
public class ESScriptUpdate extends SyncByQuery implements Serializable {

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

    public NestedObjWithId setBeforeItem(Object beforeItem) {
      this.beforeItem = beforeItem;
      return this;
    }
  }

  ESScriptUpdate(SyncData data) {
    super(data);
    outer = data;
  }

  public ESScriptUpdate updateList(String listFieldNameInEs, String syncDataFieldName) {
    Object field = outer.getField(syncDataFieldName);
    // TODO 2019-12-07 use name, convert type
    // TODO 2019-12-07 not extends SyncByQuery
//    if ()

    switch (outer.getType()) {
      case DELETE:
        remove.put(listFieldNameInEs, syncDataFieldName);
        break;
      case WRITE:
        append.put(listFieldNameInEs, syncDataFieldName);
        break;
      default:
        logger.warn("Not support update list variable for {}", outer.getType());
    }
    outer.toUpdate();
    return this;
  }

  public ESScriptUpdate updateObjectList(String listFieldNameInEs, String idName, String delta) {
    Object id = outer.getField(idName);
    switch (outer.getType()) {
      case DELETE:
        objRemove.put(listFieldNameInEs, new NestedObjWithId(id, delta));
        break;
      case WRITE:
        objAppend.put(listFieldNameInEs, new NestedObjWithId(id, delta));
        break;
      case UPDATE:
        objUpdate.put(listFieldNameInEs, new NestedObjWithId(id, delta));
        break;
      default:
        throw new UnsupportedOperationException();
    }
    outer.toUpdate();
    return this;
  }

  public boolean needScript() {
    return !append.isEmpty() || !remove.isEmpty();
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
