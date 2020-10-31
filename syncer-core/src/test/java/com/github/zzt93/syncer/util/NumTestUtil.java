package com.github.zzt93.syncer.util;

import static org.junit.Assert.*;

public class NumTestUtil {
	public static long notDoubleLong() {
		long value = ((long) Math.pow(2, 53)) + 1;
		assertNotEquals(value, (double) value);
		return value;
	}
}
