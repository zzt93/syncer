package com.github.zzt93.syncer.producer.dispatch;

import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.producer.dispatch.event.RowsEvent;

/**
 * @author zzt
 */
public interface InputFilter extends Filter<RowsEvent, FilterRes> {

}
