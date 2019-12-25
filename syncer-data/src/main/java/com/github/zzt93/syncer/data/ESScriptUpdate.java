package com.github.zzt93.syncer.data;


/**
 * update es field by script
 */
public interface ESScriptUpdate {

  /**
   * Merge relational data into json array
   *
   * @param listFieldNameInEs will use `listFieldNameInEs` one field
   * @param parentIdName      name in `fields` to remove and set as _id of ES doc for merge
   * @param toMergeFieldName  this field will be removed from `fields` and add to `listFieldNameInEs`
   * @return this instance
   * @see #mergeToListById(String, String, String)
   * @see #mergeToNestedById(String, String, String...)
   */
  @Deprecated
  ESScriptUpdate mergeToList(String listFieldNameInEs, String parentIdName, String toMergeFieldName);

  /**
   * Merge relational data into two json array with idempotent parentId and id:
   * <ul>
   * <li>`listFieldNameInEs`: [toMergeFieldName]</li>
   * <li>`listFieldNameInEs` + '_id': [id]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`, `listFieldNameInEs` + '_id' two fields
   * @param parentIdName      name in `fields` to remove and set as _id of ES doc for merge
   * @param toMergeFieldName  this field will be removed from `fields` and add to `listFieldNameInEs`
   *                          according to check of id
   * @return this instance
   */
  ESScriptUpdate mergeToListById(String listFieldNameInEs, String parentIdName, String toMergeFieldName);

  /**
   * Merge relational data into <a href=https://www.elastic.co/guide/en/elasticsearch/reference/5.4/nested.html>nested</a>
   * obj with idempotent parentId and id:
   * <ul>
   * <li>`listFieldNameInEs`: [{toMergeFieldsName: xx, id: id}]</li>
   * </ul>
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`
   * @param parentIdName      name in `fields` to remove and set as _id of ES doc for merge
   * @param toMergeFieldsName those fields will be removed from `fields` and add to nested obj
   * @return this instance
   */
  ESScriptUpdate mergeToNestedById(String listFieldNameInEs, String parentIdName, String... toMergeFieldsName);


}
