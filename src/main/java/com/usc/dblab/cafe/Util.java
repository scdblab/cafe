package com.usc.dblab.cafe;

import static com.usc.dblab.cafe.Config.KEY_PRE_PENDING_WRITES_LOG;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.meetup.memcached.COException;
import com.meetup.memcached.MemcachedClient;

public class Util {
    private static final String BW_RESERVED_STR = "xxxx";

    static String getBufferedWriteKey(String it) { 
        return String.format("BW_%s,%s", it, BW_RESERVED_STR);
    }

    static String getIndexesKey(String key) {
        return String.format("idxs_%s", key);
    }

    private static String getBufferedWriteArchiveKey(String it, int index) {
        String sIndex = getStringIndex(index);
        return String.format("BW_%s,%s", it, sIndex);
    }

    static String getBufferdWriteArchivePartitionKey(String buffKey, int index) {
        buffKey = buffKey.substring(0, buffKey.length() - BW_RESERVED_STR.length());
        String sIndex = getStringIndex(index);
        return String.format("%s%s", buffKey, sIndex);
    }

    static String getArchivedKey(String key, int idx) {
        key = key.substring(0, key.length() - BW_RESERVED_STR.length());
        String sIndex = getStringIndex(idx);
        return String.format("%s%s", key, sIndex);
    }

    static String getStringIndex(int index) {
        int max = BW_RESERVED_STR.length();
        int ndigits = 0;
        StringBuilder sb = new StringBuilder();
        int curr = index;
        
        do {
            curr /= 10;
            ndigits++;
        } while (curr != 0);
        
        if (ndigits > max) {
            System.out.println("BW_RESERVED_STR not long enough.");
            System.exit(-1);
        }
        
        for (int i = 0; i < max-ndigits; ++i) sb.append(0);
        
        sb.append(index);
        
        return sb.toString();
    }

    /**
     * Perform append or add. 
     * @throws COException 
     * @throws IOException 
     */
    static int appendOrAddSessId(String key, int hashCode, String sid, byte[] value, 
            MemcachedClient mc, boolean performAppend) throws IOException, COException {
//        String value = Config.DELIMITER + sessId;

        boolean success = false;
        if (performAppend) {
            success = mc.oqAppend(sid, key, hashCode, value, false);
            if (success) return value.length * Config.replicas;
        }

        if (!success) {
            success = mc.oqAdd(sid, key, value, hashCode, false);
            if (success) return (key.length() + value.length) * Config.replicas;
        }

        if (success == false) {   // may be: 1) key exists but full, or 2) the cache is full
            System.out.println("Append and add fail "+key);

            // check and update the buffered write indexes
            String indexesKey = getIndexesKey(key);
            Object bwIndexesVal = mc.oqRead(sid, indexesKey, hashCode, true);
            int min = 0, max = 0;
            if (bwIndexesVal != null) {                        
                String[] minmax = ((String)bwIndexesVal).split(",");
                min = Integer.parseInt(minmax[0]);
                max = Integer.parseInt(minmax[1]);
                max += 1;
            }
            success = mc.oqSwap(sid, indexesKey, hashCode, String.format("%d,%d", min, max), true);

            // swap the current value with the new changes.
            Object obj = mc.oqRead(sid, key, hashCode, false);
            success = mc.oqSwap(sid, key, hashCode, value, false);                      

            // create a new key-value pair for the old changes.
            if (obj != null) {
                String archiveKey = getArchivedKey(key, max);
                success = mc.oqSwap(sid, archiveKey, hashCode, obj, false);
            }
        }

        return (key.length() + value.length) * Config.replicas;
    }

    static LinkedHashMap<String, List<String>> getArchiveSessionIdsFromKey(String key, int hashCode, String sid, 
            MemcachedClient mc, boolean lease) throws COException {
        // check the buffered write indexes
        String indexesKey = getIndexesKey(key);
        Object indexesVal = null;

        if (lease) {
            indexesVal = mc.oqRead(sid, indexesKey, hashCode, true);
        } else {
            indexesVal = mc.get(indexesKey, hashCode, true);
        }

        String[] keys = null;
        if (indexesVal != null) {
            String[] minmax = ((String)indexesVal).split(",");
            int min = Integer.parseInt(minmax[0]);
            int max = Integer.parseInt(minmax[1]);
            int total = max - min + 1 + 1;
            keys = new String[total];
            for (int i = min; i <= max; ++i) {
                keys[i-min] = getArchivedKey(key, i);
            }
            keys[total-1] = key;
        } else {
            keys = new String[1];
            keys[0] = key;
        }

        Map<String, Object> map = null;

        if (lease) {
            map = mc.co_getMulti(sid, hashCode, 1, false, keys);
        } else {
            map = mc.getMulti(hashCode, false, keys);
        }

        LinkedHashMap<String, List<String>> sessIds = new LinkedHashMap<>();
        for (int i = 0; i < keys.length; ++i) {
            String k = keys[i];
            Object obj = map.get(k);
            if (obj == null) {
                //                System.out.println("Null value for key "+k);
            } else {
                String val = new String((byte[])obj);
                List<String> ids = MemcachedHelper.convertList(val);
                sessIds.put(k, ids);
            }
        }

        return sessIds;
    }    

    static List<String> getSessionIdsFromKey(String key, int hashCode, String sid, 
            MemcachedClient mc, boolean lease) throws COException {
        List<String> sessIds = new ArrayList<>();
        LinkedHashMap<String, List<String>> ids = getArchiveSessionIdsFromKey(key, hashCode, sid, mc, lease);
        for (List<String> list: ids.values()) {
            sessIds.addAll(list);
        }
        return sessIds;
    }

    static boolean updateSessionIdsToKey(String key, List<String> sessIds, 
            MemcachedClient mc, int hc, Stats stats, ExecutorService service, 
            NgCache cache, int id) {
        // try deque first
        // try to deque first
        Set<Integer> hcs = cache.getReplicaHashCodes(hc);
        if (Config.sendParallel) {
            Future<Boolean> fs[] = new Future[Config.replicas];
            int idx = 0;
            for (final int h: hcs) {
                final List<String> copySessIds = new ArrayList<>();
                for (String sessId: sessIds) {
                    copySessIds.add(sessId);
                }
                
                fs[idx++] = service.submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        return updateSessionIdsToKeyAsync(key, copySessIds, mc, h, stats, cache, id);
                    }
                });
            }

            Exception e = null;
            for (int r = 0; r < Config.replicas; ++r) {
                try {
                    fs[r].get();
                } catch (Exception ex) {
                    e = ex;
                }
            }
        } else {
            for (int h : hcs) {
                updateSessionIdsToKeyAsync(key, sessIds, mc, h, stats, cache, id);
            }
        }
        
        return true;
    }
    
    static boolean updateSessionIdsToKeyAsync(String key, List<String> sessIds, MemcachedClient mc, 
            int h, Stats stats, NgCache cache, int id) {
        boolean success = false;
        int diff = 0;
        if (Config.persistMode == Config.BDB_ASYNC) {
            try {
                success = mc.deque(key, id, sessIds.size(), h);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            if (success == true) {
                for (String sessId: sessIds) {
                    diff += sessId.length();
                }
                return true;
            }
        }
        
        while (true) {
            String sid = mc.generateSID();
            diff = 0;
            
            try {
                LinkedHashMap<String, List<String>> allSessIds = getArchiveSessionIdsFromKey(key, h, sid, mc, true);
                
                // try to deque first
                success = false;
                if (Config.persistMode == Config.BDB_ASYNC) {
                    try {
                        success = mc.deque(key, id, sessIds.size(), h);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    
                    if (success) {
                        for (String sessId: sessIds) {
                            diff += sessId.length();
                        }
                    }
                }
                
                if (!success) {
                    Set<String> toRemove = new HashSet<>();
                    
                    int cnt = 0;
                    for (String k: allSessIds.keySet()) {
                        List<String> ids = allSessIds.get(k);
                        boolean changed = false;
                        for (String sessId: sessIds) {
                            if (ids.remove(sessId)) {
                                diff += sessId.length() * Config.replicas;
                                cnt++;
                                changed = true;
                            }
                        }
                        if (!changed) toRemove.add(k);
                    }
                    
                    if (cnt != sessIds.size()) {
                        System.out.println("Don't find a session id in "+key);
                        System.exit(-1);
                    }
    
                    for (String k: toRemove) {
                        allSessIds.remove(k);
                    }
    
                    // update
                    if (allSessIds.size() > 0) {
                        String[] keys = allSessIds.keySet().toArray(new String[0]);
                        Object[] objs = new Object[keys.length];
                        for (int i = 0; i < keys.length; ++i) {
                            List<String> ids = allSessIds.get(keys[i]);
                            byte[] val = null;
                            if (ids != null) {
                                val = MemcachedHelper.convertString(ids).getBytes();
                            } else {
                                diff += keys[i].length() * Config.replicas;
                            }
    
                            objs[i] = val;
                        }

                        mc.oqSwaps(sid, keys, objs, h, false);
                    }
                }

                mc.dCommit(sid, h);

                if (diff > 0 && stats != null) {
                    stats.decrBy(Stats.METRIC_BW_SIZE, diff);
                }
                return true;
            } catch (Exception e) {
                try {
                    mc.dAbort(sid, h);
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
//                    e1.printStackTrace();
                }
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace(System.out);
            }
        }
    }

    public static String getEWLogKey(int ewId) {
        return KEY_PRE_PENDING_WRITES_LOG + "_w," + ewId+","+BW_RESERVED_STR;
    }
    
    public static void main(String[] args) {
        System.out.println(getArchivedKey("TW_w_1,xxxx", 0));
        System.out.println(getStringIndex(0));
        System.out.println(getStringIndex(3));
        System.out.println(getStringIndex(211));
        System.out.println(getStringIndex(001));
        System.out.println(getStringIndex(7));
    }
}
