package com.usc.dblab.cafe;

import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.usc.dblab.intervaltree.Interval1D;

/**
 * Interface for implementing cache for CADS.
 * Developer must implement the methods.
 * @author hieun
 *
 */
public abstract class CacheStore {
  /**
   * Given a dml and its impacted keys, return the changes that must be applied
   * to the values of those keys.
   * <p>
   * Note: this function does not update the cache.
   * </p>
   * @param dml The DML.
   * @param keys The impacted keys.
   * @return Set of changes, each change for each inputed key.
   */
	public abstract Map<String, Delta> updateCacheEntries(String dml, Set<String> keys);

	/**
	 * Given the query of a read, return the set of referenced keys.
	 * @param query The query of a read. 
	 * @return The referenced keys. 
	 */
	public abstract Set<String> getReferencedKeysFromQuery(String query);

	/**
	 * Given the dml of a write, return the set of impacted keys.
	 * @param dml The dml of a write.
	 * @return The impacted keys.
	 */
	public abstract Set<String> getImpactedKeysFromDml(String dml);

	/**
	 * Given a query, applying it to the data store and return the result.
	 * @param query A query.
	 * @return The result of executing the query to the data store.
	 */
	public abstract QueryResult queryDataStore(String query) throws Exception;

	/**
	 * Given the query result, compute the cache entries to be inserted in the cache.
	 * @param query A query.
	 * @param result The result of executing the query to the data store. 
	 * @return The corresponding cache entries.
	 */
	public abstract Set<CacheEntry> computeCacheEntries(String query, QueryResult result);

	/**
	 * Given a dml, executing it to the data store.
	 * @param dml A dml.
	 * @return <p>true</p> if the dml was executed successfully. Otherwise return <p>false</p>.
	 */
	public abstract boolean dmlDataStore(String dml) throws Exception;

	/**
	 * Given a cache entry and its change, return the new cache entry that has
	 * been applied the change.
	 * @param change A change.
	 * @param cacheEntry The corresponding cache entry.
	 * @return A new cache entry that includes the change.
	 */
	public abstract CacheEntry applyDelta(Delta delta, CacheEntry cacheEntry);
	
	/**
	 * Given a cache entry, serialize it to a byte array.
	 * @param cacheEntry A cache entry.
	 * @return The byte array.
	 */
	public abstract byte[] serialize(CacheEntry cacheEntry);
	
	/**
	 * Given a byte array and a key, de-serialize the byte array to get the CacheEntry object.
	 * Based on the type of the key, the byte array may be de-serialized differently.
	 * @param key A key.
	 * @param bytes A byte array.
	 * @param buffer A read buffer.
	 * @return The CacheEntry object after de-serialize the byte array.
	 */
	public abstract CacheEntry deserialize(String key, Object obj, byte[] buffer);
	
    /**
     * Given a change of type Append, describes how to serialize it.
     * @param change A change.
     * @return The byte array.
     */
    public abstract byte[] serialize(Delta change);
	
	/**
	 * Given a key, return its hash code. This is used to partition the keys 
	 * across multiple cache servers.
	 * @param key A key.
	 * @return The hash code value of the key.
	 */
	public abstract int getHashCode(String key);

	/**
	 * Given a set of cache entries, get the query result.
	 * @param query
	 * @param entries
	 * @return
	 */
	public abstract QueryResult computeQueryResult(String query, Set<CacheEntry> entries);

    public String getCollectionName(String query) {
        return null;
    }

    public Interval1D getBounds(String queryOrDml) {
        return null;
    }

    public String constructFixPointQuery(String query, Object... params) {
        return null;
    }

    public Map<Interval1D, List<Delta>> updatePoints(String dml) {
        // TODO Auto-generated method stub
        return null;
    }
}
