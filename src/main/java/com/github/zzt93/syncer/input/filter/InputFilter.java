package com.github.zzt93.syncer.input.filter;

import com.github.zzt93.syncer.common.MysqlRowEvent;
import com.github.zzt93.syncer.filter.Filter;
import com.github.zzt93.syncer.filter.Filter.FilterRes;

/**
 * @author zzt
 */
public interface InputFilter extends Filter<MysqlRowEvent, FilterRes> {

}
