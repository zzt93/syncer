package com.github.zzt93.syncer.data;


/**
 * update es field by script
 */
public interface ESScriptUpdate {


  ESScriptUpdate updateList(String listFieldNameInEs, String syncDataFieldName);

  ESScriptUpdate updateListById(String listFieldNameInEs, String syncDataFieldName);


}
