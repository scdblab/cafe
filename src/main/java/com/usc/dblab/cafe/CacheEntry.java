package com.usc.dblab.cafe;

public class CacheEntry {
	private final String key;
	private final Object value;
	
	private final boolean asString;
	
	public String getKey() {
		return key;
	}
	
	public Object getValue() {
		return value;
	}
	
	public boolean asString() {
	    return asString;
	}
	
	public CacheEntry(String key, Object value, boolean asString) {
		this.key = key;
		this.value = value;
		this.asString = asString;
	}
}