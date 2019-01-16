package com.usc.dblab.cafe;


import static com.usc.dblab.cafe.Config.DELIMITER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.meetup.memcached.MemcachedClient;

public class MemcachedHelper {
	private static Logger logger = Logger.getLogger(MemcachedHelper.class);

	public static Set<String> convertSet(Object obj) {
		if (obj == null) {
			return null;
		}
		String strVal = String.valueOf(obj);
		if (strVal.contains(":")) 
			strVal = strVal.substring(strVal.indexOf(":")+1);

		String[] vals = strVal.split(DELIMITER);
		Set<String> set = new HashSet<>();
		for (String val : vals) {
			if (!val.isEmpty())
				set.add(val);
		}
		return set;
	}

	public static List<String> convertList(Object obj) {
		if (obj == null) {
			return null;
		}
		String strVal = String.valueOf(obj);

		if (strVal.contains(":")) 
			strVal = strVal.substring(strVal.indexOf(":")+1);

		String[] vals = strVal.split(DELIMITER);
		List<String> list = new ArrayList<>();
		for (String val : vals) {
			if (!val.isEmpty())
				list.add(val);
		}
		return list;
	}

	public static String convertString(Collection<String> list) {
		if (list.isEmpty()) {
//			return DELIMITER;
		    return null;
		}

		StringBuilder builder = new StringBuilder();
		list.stream().forEach(i -> {
			builder.append(i);
			builder.append(DELIMITER);
		});
		return builder.toString();
	}

	public static boolean addToSet(MemcachedClient memcachedClient, 
			String key, Integer hashCode, 
			String valueToAdd,
			boolean addIfAppendFailed) {
		boolean ret = false;
		try {
			ret = memcachedClient.append(key, hashCode, valueToAdd + DELIMITER);
			if (!ret && addIfAppendFailed) {
				ret = memcachedClient.add(key, valueToAdd + DELIMITER, hashCode);
			}
		} catch (Exception e) {
			logger.error("Failed to add key to set. key: " + key + " value: " + valueToAdd, e);
			return ret;
		}
		return ret;
	}

	public static void set(MemcachedClient memcachedClient, String key, Integer hashCode, Set<String> list) {
		try {
			StringBuilder builder = new StringBuilder();
			builder.append(",");
			for (String value : list) {
				builder.append(value);
				builder.append(DELIMITER);
			}
			memcachedClient.set(key, builder.toString(), hashCode);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/**
	 * @param memcachedClient
	 * @param key
	 * @param valueToAdd
	 * @param addIfAppendFailed
	 * @return the result of append, true if key exists and append success.
	 *         false means key doesn't exist.
	 */
	public static boolean addToSetAppend(MemcachedClient memcachedClient, String key, Integer hashCode,
			String valueToAdd,
			boolean addIfAppendFailed) {
		boolean appendRet = false;
		boolean addRet = false;
		try {
			appendRet = memcachedClient.append(key, hashCode, valueToAdd + DELIMITER);
			if (!appendRet && addIfAppendFailed) {
				addRet = memcachedClient.add(key, valueToAdd + DELIMITER, hashCode);
				return appendRet;
			}
		} catch (Exception e) {
			logger.error("Failed to add key to set. key: " + key + " value: " + valueToAdd, e);
			return appendRet;
		} finally {
			if (appendRet == false && addRet == false) {
				System.out.println("BUG!!!" + key);
			}
		}
		return appendRet;
	}

	public static boolean removeFromSet(MemcachedClient memcachedClient, 
			String key, Integer hashCode, String valueToRemove, String delimiter) {

		Object val = memcachedClient.get(key, hashCode, true);
		if (val == null) {
			return false;
		}
		Set<String> values = convertSet(val);
		if (!values.remove(valueToRemove)) {
			logger.fatal("BUG: key " + key + " val: " + val + " " + values + " " + valueToRemove);
		}
		try {
			return memcachedClient.set(key, convertString(values), hashCode);
		} catch (Exception e) {
			logger.error("Failed to remove value from set. key: " + key + " value: " + valueToRemove, e);
		}

		return false;
	}

	public static void main(String[] args) {
		System.out.println((char) (1 + 'a'));
	}
}
