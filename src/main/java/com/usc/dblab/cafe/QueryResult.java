package com.usc.dblab.cafe;

public abstract class QueryResult {
	String query;
	
	public QueryResult(String query) {
		this.query = query;
	}
	
	public String getQuery() { return query; }
}
