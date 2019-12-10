package com.github.zzt93.syncer.data;


/**
 * update es field by script
 */
public interface ESScriptUpdate {

  /**
   * Merge relational data into flatten json
   * @param syncDataFieldName this field will be removed from `fields`
   */
  @Deprecated
  ESScriptUpdate mergeToList(String listFieldNameInEs, String syncDataFieldName);

  /**
   * Merge relational data into flatten json with idempotent id
   * @param syncDataFieldName this field will be removed from `fields`
   */
  ESScriptUpdate mergeToListById(String listFieldNameInEs, String syncDataFieldName);


}
