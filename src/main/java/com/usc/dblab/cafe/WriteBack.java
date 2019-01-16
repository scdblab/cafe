package com.usc.dblab.cafe;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.usc.dblab.ngcache.dendrite.BufferedWriteItem;

import edu.usc.dblab.intervaltree.Interval1D;

/**
 * Interface for write-back policy.
 * Developer must implement the methods to support write-back policy.
 * @author hieun
 *
 */
public abstract class WriteBack {
  /**
   * Given a key of a cache entry, return key(s) of buffered write(s) that impact its value.  
   * @param key Identifier of a cache entry.
   * @return Key(s) of buffered write(s) that impact the value of this key.
   */
	public abstract Set<String> getMapping(String key);
	
	/**
	 * This optional method implements the necessary data structure to provide a mapping.  
	 * It is optional when the key of buffered writes is a string manipulation of the key of the cached entry.  
	 * To support range predicates, one may want to implement BuWrInT of RangeQC to map values impacted 
	 * by a buffered write to the key of the buffered write.  One may do so using this method.
	 * @param buffKeys A set of keys where each key identifies a buffered write.
	 * @return <p>true</p> if 
	 */
	public boolean registerBuffKeysToMapping(Set<String> buffKeys) throws Exception {
	  return true;
	}

	/**
	 * This optional method is the inverse of the registerMapping.  
	 * It is used to repair developer authored data structures that map values to buffered writes. 
	 * For example, with BuWrInT of RangeQC, once a background thread applies a buffered write to the data store, 
	 * it may call this method to delete the association of impacted values to the key of buffered write. 
	 * The software for this is provided by the application developer. 
	 * @param key A key.
	 * @return
	 */
	public boolean unregisterBuffKeysFromMapping(String key) throws Exception {
	  return true;
	}
	
	/**
	 * Given a dml and its impacted buffered write keys, return the changes
	 * that must be applied to the values of the buffered write keys.
	 * @param dml A dml.
	 * @param buffKeys The impacted buffered write keys.
	 * @return Changes that must be applied to the buffered write keys, one per key.
	 */
	public abstract LinkedHashMap<String, Change> bufferChanges(String dml, Set<String> buffKeys); 
	
	/**
	 * Given a buffered write key and its value, apply it to the data store.
	 * @param buffKey A buffered key.
	 * @param buffValue Its value.
	 * @return <p>true</p> if applied successfully. Otherwise, return <p>false</p>.
	 */
	public abstract boolean applyBufferedWrite(String buffKey, 
	    Object buffValue);
	
	/**
	 * Check whether a buffered write value is idempotent or not.
	 * @param buffValue A buffered write value.
	 * @return <p>true</p> if it is idempotent, <p>false</p> otherwise.
	 */
	public abstract boolean isIdempotent(Object buffValue);

	/**
	 * Convert a non-idempotent buffered write value to idempotent.
	 * @param buffValue A buffered write value.
	 * @return The idempotent one.
	 */
	public abstract Object convertToIdempotent(Object buffValue);
	
	/**
	 * Given a query, return the list of data item identifiers this query references.
	 * @param query
	 * @return
	 */
	public abstract Set<String> rationalizeRead(String query);
	
	/**
	 * Given a dml, return the list of changes that must be buffered in the cache with write-back policy.
	 * @param dml
	 * @return
	 */
	public abstract LinkedHashMap<String, Change> rationalizeWrite(String dml);
	
	
	/**
     * Given a change of type Append, describes how to serialize it.
     * @param change A change.
     * @return The byte array.
     */
    public abstract byte[] serialize(Change change);
	
	/**
	 * Deserialize a change
	 * @param bytes
	 * @param read_buffer
	 * @return
	 */
	public abstract Change deserialize(byte[] bytes);

	/**
	 * Apply the provided list of sessions as one transaction to the data store.
	 * @param sess
	 * @param stmt 
	 * @param sessionWriter 
	 * @param stats 
	 */
	public abstract boolean applySessions(List<Session> sess, Connection conn, 
	        Statement stmt, PrintWriter sessionWriter, Stats stats) throws Exception;

	/**
	 * Merge the query result with the pending buffered write to produce the latest value
	 * @param result
	 * @param buffVals
	 * @return
	 */
    public abstract QueryResult merge(String query, QueryResult result, LinkedHashMap<String, List<Change>> buffVals);

    /**
     * Serialize changes of a session into bytes.
     * @param changesMap
     * @return
     */
	public abstract byte[] serializeSessionChanges(Map<String, List<Change>> changesMap);

	/**
	 * Get the teleW partition.
	 */
	public abstract int getTeleWPartition(String sessId);

	/**
	 * De-serialize changes
	 * @param bytes
	 * @return
	 */
	public abstract Map<String, List<Change>> deserializeSessionChanges(byte[] bytes);
	
	
	public boolean createSessionTable() {
	    return Config.storeCommitedSessions ? false : true;
	}
	
	public boolean insertCommitedSessionRows(List<String> sessId) {
	    return Config.storeCommitedSessions ? false : true;
	}

    public boolean cleanupSessionTable(List<String> appliedSessIds) {
        return Config.storeCommitedSessions ? false : true;
    }

    public List<String> checkExists(List<String> toExec) {
        return null;
    }

    public void merge(String newQuery, QueryResult result,
            Map<Integer, List<BufferedWriteItem>> items) {
        // TODO Auto-generated method stub
        
    }

    public Map<Interval1D, List<Change>> bufferPoints(String dml) {
        // TODO Auto-generated method stub
        return null;
    }

    public Map<Interval1D, String> getImpactedRanges(Session sess) {
        // TODO Auto-generated method stub
        return null;
    }
}
