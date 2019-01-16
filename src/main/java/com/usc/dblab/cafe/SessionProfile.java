package com.usc.dblab.cafe;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SessionProfile {
    public static final String METRIC_QUERIES = "queries";
    public static final String METRIC_CIGET = "ciget";
    public static final String METRIC_IQSET = "iqset";
    public static final String METRIC_COGETS_C = "cogets_c";
    public static final String METRIC_COGETS_O = "cogets_o";
    public static final String METRIC_OQAPPEND = "oqappend";
    public static final String METRIC_OQREAD = "oqread";
    public static final String METRIC_OQSWAP = "oqswap";
    public static final String METRIC_OQSWAPS = "oqswaps";
    public static final String METRIC_KEYS_IN_OQSWAPS = "keys_oqswaps";
    public static final String METRIC_KEYS_IN_COGETS_C = "keys_cogets_c";
    public static final String METRIC_KEYS_IN_COGETS_O = "keys_cogets_o";
    public static final String METRIC_SESSION_STARTS = "session_start";
    public static final String METRIC_SESSION_COMMITS = "session_commit";
    public static final String METRIC_SESSION_ABORTS = "session_aboort";
    public static final String METRIC_OQINCR = "oqincr";
    public static final String METRIC_OQDECR = "oqdecr";
    public static final String METRIC_OQWRITE = "oqwrite";
    public static final String METRIC_DMLS = "dmls";
    public static final String METRIC_OQADD = "oqadd";
    public static final String METRIC_OQAPPENDS = "oqappends";
    public static final String METRIC_KEYS_IN_OQAPPENDS = "keys_oqappends";

    boolean enabled;
    
    final Map<String, Map<String, AtomicInteger>> statsMap;
    
    private Map<String, AtomicInteger> currMap;
    
    public SessionProfile(boolean profile) {
        statsMap = new ConcurrentHashMap<>();
        enabled = profile;
    }

    public void init(String name) {
        if (name == null) {
            currMap = null;
            return;
        }
        
        Map<String, AtomicInteger> map = statsMap.get(name);
        if (map == null) {
            map = new ConcurrentHashMap<>();
            statsMap.put(name, map);
        }
        currMap = map;
    }
    
    public void incr(String metric) {
        incrBy(metric, 1);
    }
    
    public void incrBy(String metric, int i) {
        if (!enabled) return;
        if (currMap == null) return;
        
        AtomicInteger old_val = currMap.putIfAbsent(metric, new AtomicInteger(i));
        if (old_val != null) {
            old_val.addAndGet(i);
        }
    }
    
    public synchronized void aggregate(Map<String, Map<String, Integer>> total) {
        for (String name: statsMap.keySet()) {
            Map<String, Integer> map = total.get(name);
            if (map == null) {
                map = new HashMap<>();
                total.put(name, map);
            }
            
            for (String key: statsMap.get(name).keySet()) {
               Integer preVal = map.get(key);
               AtomicInteger addedVal = statsMap.get(name).get(key);
               if (addedVal != null) {
                   if (preVal != null) {
                       map.put(key, preVal.intValue()+addedVal.intValue());
                   } else {
                       map.put(key, addedVal.intValue());
                   }
               }
            }
        }
    }
}