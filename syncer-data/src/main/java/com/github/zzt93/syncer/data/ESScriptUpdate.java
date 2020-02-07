package com.github.zzt93.syncer.data;


import java.util.Map;

/**
 * update es field by script
 */
public interface ESScriptUpdate {

  /**
   * Merge relational data into json array
   *
   * @param listFieldNameInEs will use `listFieldNameInEs` one field
   * @param parentIdName      name of a field in `fields` whose value will be used as `_id` to query a ES parent doc, then for merge. This field will be removed from `fields` automatically
   * @param toMergeFieldName  this field will be removed from `fields` and its value will be added to `listFieldNameInEs`
   * @return this instance
   * @see #mergeToListById(String, String, String)
   * @see #mergeToNestedById(String, String, String...)
   */
  @Deprecated
  ESScriptUpdate mergeToList(String listFieldNameInEs, String parentIdName, String toMergeFieldName);

  /**
   * Merge relational data into two json array with idempotent parentId and id:
   * <ul>
   * <li>`listFieldNameInEs`: [toMergeField]</li>
   * <li>`listFieldNameInEs` + '_id': [id]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`, `listFieldNameInEs` + '_id' two fields
   * @param parentIdName      name of a field in `fields` whose value will be used as `_id` to query a ES parent doc, then for merge. This field will be removed from `fields` automatically
   * @param toMergeFieldName  this field will be removed from `fields` and its value will be  add to `listFieldNameInEs`
   *                          according to check of id
   * @return this instance
   */
  ESScriptUpdate mergeToListById(String listFieldNameInEs, String parentIdName, String toMergeFieldName);

  /**
   * Merge relational data into <a href=https://www.elastic.co/guide/en/elasticsearch/reference/5.4/nested.html>nested</a>
   * obj with idempotent parentId and id:
   * <ul>
   * <li>`listFieldNameInEs`: [{toMergeField1Name: toMergeField1Value, toMergeField2Name: toMergeField2Value, id: xx}]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`
   * @param parentIdName      name of a field in `fields` whose value will be used as `_id` to query a ES parent doc, then for merge. This field will be removed from `fields` automatically
   * @param toMergeFieldsName fields to be added to nested obj as json format (primary key is always included). Those fields will be removed from `fields` automatically
   * @return this instance
   */
  ESScriptUpdate mergeToNestedById(String listFieldNameInEs, String parentIdName, String... toMergeFieldsName);

  /**
   * Merge relational data into <a href=https://www.elastic.co/guide/en/elasticsearch/reference/5.4/nested.html>nested</a>
   * obj with parent filter and id:
   * <ul>
   * <li>`listFieldNameInEs`: [{toMergeField1Name: toMergeField1Value, toMergeField2Name: toMergeField2Value, id: xx}]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`
   * @param parentFilter      filter for query a ES parent doc, will be added to `syncByQuery`
   * @param toMergeFieldsName fields to be added to nested obj as json format (primary key is always included). Those fields will be removed from `fields` automatically
   * @return this instance
   * @see SyncData#syncByQuery()
   * @see SyncByQuery
   */
  ESScriptUpdate mergeToNestedByQuery(String listFieldNameInEs, Map<String, Object> parentFilter, String... toMergeFieldsName);

  /**
   * Merge relational data into <a href=https://www.elastic.co/guide/en/elasticsearch/reference/5.4/nested.html>nested</a>
   * obj with parent filter and id:
   * <ul>
   * <li>`listFieldNameInEs`: [{toMergeField1Name: toMergeField1Value, toMergeField2Name: toMergeField2Value, id: xx}]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`
   * @param parentFilterKey   filter name for query a ES parent doc, will be added to `syncByQuery`
   * @param parentFilterValue filter value for query a ES parent doc, will be added to `syncByQuery`
   * @param toMergeFieldsName fields to be added to nested obj as json format (primary key is always included). Those fields will be removed from `fields` automatically
   * @return this instance
   * @see SyncData#syncByQuery()
   * @see SyncByQuery
   */
  ESScriptUpdate mergeToNestedByQuery(String listFieldNameInEs, String parentFilterKey, Object parentFilterValue, String... toMergeFieldsName);


}
