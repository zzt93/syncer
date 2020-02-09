package com.github.zzt93.syncer.data;


/**
 * update es field by script
 */
public interface ESScriptUpdate {

  /**
   * Merge relational data into json array
   *
   * @param listFieldNameInEs will use `listFieldNameInEs` one field
   * @param toMergeFieldName  this field will be removed from `fields` and its value will be added to `listFieldNameInEs`
   * @return this instance
   * @see #mergeToListById(String, String)
   * @see #mergeToNestedById(String, String...)
   */
  @Deprecated
  ESScriptUpdate mergeToList(String listFieldNameInEs, String toMergeFieldName);

  /**
   * Merge relational data into two json array with idempotent parentId and id:
   * <ul>
   * <li>`listFieldNameInEs`: [toMergeField]</li>
   * <li>`listFieldNameInEs` + '_id': [id]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`, `listFieldNameInEs` + '_id' two fields
   * @param toMergeFieldName  this field will be removed from `fields` and its value will be  add to `listFieldNameInEs`
   *                          according to check of id
   * @return this instance
   */
  ESScriptUpdate mergeToListById(String listFieldNameInEs, String toMergeFieldName);

  /**
   * Merge relational data into <a href=https://www.elastic.co/guide/en/elasticsearch/reference/5.4/nested.html>nested</a>
   * obj with idempotent parentId and id:
   * <ul>
   * <li>`listFieldNameInEs`: [{toMergeField1Name: toMergeField1Value, toMergeField2Name: toMergeField2Value, id: xx}]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`
   * @param toMergeFieldsName fields to be added to nested obj as json format (primary key is always included). Those fields will be removed from `fields` automatically
   * @return this instance
   */
  ESScriptUpdate mergeToNestedById(String listFieldNameInEs, String... toMergeFieldsName);

  /**
   * Update/Delete child doc of <a href=https://www.elastic.co/guide/en/elasticsearch/reference/5.4/nested.html>nested</a>
   * obj with parent filter and child filter:
   * <ul>
   * <li>`listFieldNameInEs`: [{toMergeField1Name: toMergeField1Value, toMergeField2Name: toMergeField2Value, id: xx}]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`
   * @param childFilter       filter for query a ES children doc under a parent doc, will be used in ES update script
   * @param toMergeFieldsName fields to be added to nested obj as json format (primary key is always included). Those fields will be removed from `fields` automatically
   * @return this instance
   * @see SyncData#syncByQuery()
   * @see SyncByQuery
   */
  ESScriptUpdate mergeToNestedByQuery(String listFieldNameInEs, Filter childFilter, String... toMergeFieldsName);


}
