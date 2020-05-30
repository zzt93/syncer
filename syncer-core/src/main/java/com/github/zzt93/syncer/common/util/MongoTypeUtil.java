package com.github.zzt93.syncer.common.util;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class MongoTypeUtil {
	public static Object convertBsonTypes(Object o) {
		if (o instanceof Map) {
			if (((Map) o).size() == 1 && ((Map) o).containsKey("$numberDecimal")) {
				return new BigDecimal((String) ((Map) o).get("$numberDecimal"));
			}
			for (Map.Entry<String, Object> e : ((Map<String, Object>) o).entrySet()) {
				e.setValue(convertBsonTypes(e.getValue()));
			}
		} else if (o instanceof List) {
			List list = (List) o;
			for (int i = 0; i < list.size(); i++) {
				list.set(i, convertBsonTypes(list.get(i)));
			}
		} else if (o instanceof Binary) {
			return ((Binary) o).getData();
		} else if (o instanceof Decimal128) {
			return ((Decimal128) o).bigDecimalValue();
		} else if (o instanceof BsonTimestamp) {
			return convertBson((BsonValue) o);
		} else if (o instanceof ObjectId) {
			return o.toString();
		}
		return o;
	}

	public static Object convertBson(BsonValue value) {
		switch (value.getBsonType()) {
			case INT64:
				return value.asInt64().getValue();
			case INT32:
				return value.asInt32().getValue();
			case BINARY:
				return value.asBinary().getData();
			case DOUBLE:
				return value.asDouble().getValue();
			case STRING:
				return value.asString().getValue();
			case BOOLEAN:
				return value.asBoolean().getValue();
			case DATE_TIME:
				return new Date(value.asDateTime().getValue());
			case DECIMAL128:
				return value.asDecimal128().getValue().bigDecimalValue();
			case OBJECT_ID:
				return value.asObjectId().toString();
			case NULL:
				return null;
			case ARRAY:
				List<BsonValue> values = value.asArray().getValues();
				List<Object> list = new ArrayList<>();
				for (BsonValue bsonValue : values) {
					list.add(convertBson(bsonValue));
				}
				return list;
			case DOCUMENT:
				HashMap<String, Object> map = new HashMap<>();
				for (Map.Entry<String, BsonValue> o : value.asDocument().entrySet()) {
					map.put(o.getKey(), convertBson(o.getValue()));
				}
				return map;
			case TIMESTAMP:
			default:
				return value;
		}
	}
}
