package com.github.zzt93.syncer.common.util;

import com.github.zzt93.syncer.common.data.ExtraQuery;
import com.github.zzt93.syncer.common.data.ExtraQueryField;
import org.springframework.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncDataTypeUtil {

	/**
	 * @param mapping unmodifiable structure of json
	 * @param res result map
	 */
	public static void mapTo(final Map<String, Object> mapping, Map<String, Object> res) {
		for (String key : mapping.keySet()) {
			Object value = mapping.get(key);
			if (value instanceof Expression) {
				throw new UnsupportedOperationException();
			} else if (value instanceof Map) {
				Map map = (Map) value;
				mapObj(res, key, map);
			} else if (value instanceof ExtraQuery) {
				throw new UnsupportedOperationException();
			} else if (value instanceof ExtraQueryField) {
				res.put(key, ((ExtraQueryField) value).getQueryResult(key));
			} else {
				res.put(key, value);
			}
		}
	}

	private static void mapObj(Map<String, Object> res, String objKey, Map objMap) {
		HashMap<String, Object> sub = new HashMap<>();
		mapTo(objMap, sub);
		res.put(objKey, sub);
	}

	private static Object convert(String key, Object value) {
		if (value instanceof Expression) {
			throw new UnsupportedOperationException();
		} else if (value instanceof Map) {
			Map<String, Object> map = (Map<String, Object>) value;
			for (Map.Entry<String, Object> e : map.entrySet()) {
				e.setValue(convert(e.getKey(), e.getValue()));
			}
			return map;
		} else if (value instanceof List) {
			List l = (List) value;
			for (int i = 0; i < l.size(); i++) {
				l.set(i, convert(null, l.get(i)));
			}
			return value;
		} else if (value instanceof ExtraQuery) {
			return ((ExtraQuery) value).getQueryResult(key);
		} else if (value instanceof ExtraQueryField) {
			return ((ExtraQueryField) value).getQueryResult(key);
		}
		return value;
	}

	public static void convert(Object value) {
		convert(null, value);
	}
}
