package com.github.zzt93.syncer.common.util;

import com.github.zzt93.syncer.common.data.ExtraQuery;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import org.springframework.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncDataTypeUtil {

	public static final String ROW_ALL = "fields.*";
	public static final String ROW_FLATTEN = "fields.*.flatten";
	public static final String EXTRA_ALL = "extra.*";
	public static final String EXTRA_FLATTEN = "extra.*.flatten";

	/**
	 * todo remove field mapping, left it for user filter code
	 *
	 * @param context parse context
	 * @param mapping unmodifiable structure of json
	 * @param res result map
	 * @param interpretSpecialString whether include some special string
	 *
	 * @see SyncDataTypeUtil#ROW_ALL
	 * @see SyncDataTypeUtil#ROW_FLATTEN
	 */
	public static void mapToJson(SyncData context, final Map<String, Object> mapping, Map<String, Object> res,
															 boolean interpretSpecialString) {
		for (String key : mapping.keySet()) {
			Object value = mapping.get(key);
			if (value instanceof Expression) {
				res.put(key, ((Expression) value).getValue(context.getContext()));
			} else if (value instanceof Map) {
				Map map = (Map) value;
				mapObj(context, res, key, map, interpretSpecialString);
			} else if (value instanceof String && interpretSpecialString) {
				String expr = (String) value;
				switch (expr) {
					case ROW_ALL:
						mapObj(context, res, key, context.getFields(), false);
						break;
					case EXTRA_ALL:
						res.put(key, context.getExtras());
						break;
					case ROW_FLATTEN:
						mapToJson(context, context.getFields(), res, false);
						break;
					case EXTRA_FLATTEN:
						res.putAll(context.getExtras());
						break;
					default:
						throw new InvalidConfigException("Unknown special expression: " + expr);
				}
			} else if (value instanceof ExtraQuery) {
				res.put(key,  ((ExtraQuery) value).getQueryResult(key));
			} else {
				res.put(key, value);
			}
		}
	}

	private static void mapObj(SyncData src, Map<String, Object> res, String objKey, Map objMap,
														 boolean interpretSpecialString) {
		HashMap<String, Object> sub = new HashMap<>();
		mapToJson(src, objMap, sub, interpretSpecialString);
		res.put(objKey, sub);
	}

	private static Object convert(SyncData context, Object value, String key) {
		if (value instanceof Expression) {
			return ((Expression) value).getValue(context.getContext());
		} else if (value instanceof Map) {
			Map<String, Object> map = (Map<String, Object>) value;
			for (Map.Entry<String, Object> e : map.entrySet()) {
				e.setValue(convert(context, e.getValue(), e.getKey()));
			}
			return map;
		} else if (value instanceof List) {
			List l = (List) value;
			for (int i = 0; i < l.size(); i++) {
				l.set(i, convert(context, l.get(i), null));
			}
			return value;
		} else if (value instanceof ExtraQuery) {
			return ((ExtraQuery) value).getQueryResult(key);
		}
		return value;
	}

	public static void convert(Object value) {
		convert(null, value, null);
	}
}
