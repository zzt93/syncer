package com.github.zzt93.syncer.data;


/**
 * update es field by script
 */
public interface ESScriptUpdate {

  /**
   * Merge relational data into flatten json
   * @param listFieldNameInEs will use `listFieldNameInEs` one field
   * @param toMergeFieldNameInSyncData this field will be removed from `fields` and add to `listFieldNameInEs`
   * @return this instance
   */
  @Deprecated
  ESScriptUpdate mergeToList(String listFieldNameInEs, String toMergeFieldNameInSyncData);

  /**
   * Merge relational data into flatten json with idempotent parentId & id
   * @param listFieldNameInEs will use `listFieldNameInEs`, `listFieldNameInEs` + '_id' two fields
   * @param parentIdName name in `fields` to remove and add to `listFieldNameInEs` + '_id' list
   * @param toMergeFieldNameInSyncData this field will be removed from `fields` and add to `listFieldNameInEs`
   *                          according to check of id
   * @return this instance
   */
  ESScriptUpdate mergeToListById(String listFieldNameInEs, String parentIdName, String toMergeFieldNameInSyncData);

  /**
   * Merge relational data into flatten json with idempotent parentId & id
   *
   * @param listFieldNameInEs will use `listFieldNameInEs`
   * @param parentIdName name in `fields` to remove and used in nested obj
   * @param toMergeFieldsNameInSyncData those fields will be removed from `fields` and add to nested obj
   * @return this instance
   */
  ESScriptUpdate mergeToNestedById(String listFieldNameInEs, String parentIdName, String... toMergeFieldsNameInSyncData);


}
