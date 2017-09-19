package com.github.zzt93.syncer.input.filter;

import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.event.RowEvent;

/**
 * @author zzt
 */
public interface InputFilter extends Filter<RowEvent, FilterRes> {

}
