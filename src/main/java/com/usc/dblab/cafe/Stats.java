package com.usc.dblab.cafe;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.json.JSONObject;

/**
 * Collect stats per thread.
 * Not thread-safe.
 * @author hieun
 *
 */
public class Stats {
    private long[] statsArr;
    private Map<String, Integer> stats2Index;
    private int numOfStats;

    public static Map<Integer, Stats> statsMap = new ConcurrentHashMap<>();
    
    public static final String METRIC_CACHE_HITS = "cache_hits";
    public static final String METRIC_CACHE_MISSES = "cache_misses";
    public static final String METRIC_READ_STATEMENTS = "read_statements";
    public static final String METRIC_WRITE_STATEMENTS = "write_statements";
    public static final String METRIC_SESSIONS = "sessions";
    public static final String METRIC_QUERY_NO_CACHE = "query_no_cache";
    public static final String METRIC_APPLIED_SESSIONS = "applied_sessions";
    public static final String METRIC_BUFFERED_SESSIONS = "buffered_sessions";
    public static final String METRIC_COMMITED_SESSIONS = "commited_sessions";
    public static final String METRIC_ABORTED_SESSIONS = "aborted_sessions";
    
    public static final String METRIC_AR_COMMIT_SESSONS = "ar_commit_sessions";
    public static final String METRIC_AR_ABORT_SESSIONS = "ar_abort_sessions";
    public static final String METRIC_BW_SIZE = "bw_size";
    public static final String METRIC_BW_SKV_SIZE = "bw_skv_size";
    
    
    public static volatile boolean stats = true; 
    
    final static Logger logger = Logger.getLogger(Stats.class);
    
    public static Stats getStatsInstance(int threadId) {
        Stats stats = new Stats();
        Stats old_stats = statsMap.putIfAbsent(threadId, stats);
        if (old_stats != null) {
            stats = old_stats;
        }
        return stats;
    }
    
    private Stats() {
        reset();
    }
    
    public void incr(String metric) {
        incrBy(metric, 1);
    }
    
    public void incrBy(String metric, long value) {
        if (stats) {
            int idx = getMetricIndex(metric);
            statsArr[idx] += value;
        }
    }
    
    public void decr(String metric) {
        decrBy(metric, 1);
    }
    
    public void decrBy(String metric, long value) {
        if (stats) {
            int idx = getMetricIndex(metric);
            statsArr[idx] -= value;
        }
    }
    
    private int getMetricIndex(String metric) {
        Integer idx = stats2Index.get(metric);
        if (idx == null) {
            if (statsArr.length <= numOfStats) {
                long[] newStats = new long[statsArr.length*2];
                for (int i = 0; i < statsArr.length; i++) {
                    newStats[i] = statsArr[i];
                }
                statsArr = newStats;
            }
            stats2Index.put(metric, numOfStats);
            return numOfStats++;
        }
        return idx;
    }

    public Map<String, Long> getStats() {
        Map<String, Long> map = new HashMap<>();
        
        for (String metric: stats2Index.keySet()) {
            int idx = stats2Index.get(metric);
            if (statsArr.length <= idx) {
                logger.error(
                        String.format("Cannot find metric %s index %d in array.", metric, idx));
                continue;
            }
            map.put(metric, statsArr[idx]);
        }
        
        return map;
    }
    
    public static JSONObject getAllStats() {
        Map<String, Long> res = new HashMap<>();
        
        for (Stats stats: statsMap.values()) {
            Map<String, Long> map = stats.getStats();
            for (String metric: map.keySet()) {
                Long x = map.get(metric);
                Long val = res.get(metric);
                if (val == null) {
                    val = x;
                } else {
                    val = val+x;
                }
                res.put(metric, val);
            }
        }
        
        JSONObject jobj = new JSONObject();
        
        for (String metric: res.keySet()) {
            jobj.put(metric, res.get(metric));
        }
        
        return jobj;
    }
    
    public static long get(String metric) {
        long val = 0;
        for (Stats s: statsMap.values()) {
            int idx = s.getMetricIndex(metric);
            val += s.statsArr[idx];
        }
        return val;
    }

    public void reset() {
        numOfStats = 0;
        stats2Index = new ConcurrentHashMap<>();
        statsArr = new long[10];
    }
}
