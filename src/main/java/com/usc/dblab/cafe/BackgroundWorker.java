package com.usc.dblab.cafe;

import static com.usc.dblab.cafe.Config.NUM_PENDING_WRITES_LOGS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.meetup.memcached.COException;
import com.meetup.memcached.MemcachedClient;
import com.usc.dblab.ngcache.dendrite.Dendrite;

import edu.usc.dblab.intervallock.IntervalLock;
import edu.usc.dblab.intervaltree.Interval1D;

/**
 * 
 * @author hieun
 *
 */
public class BackgroundWorker extends Thread {
    private final MemcachedClient mc;
    private final int id;
    private volatile boolean isRunning;
    private NgCache cache;
    private final Stats stats;
    private Connection conn;
    private Statement stmt;

    private static Random rand = new Random();
    private static PrintWriter sessWriter = null;

    byte[] buffer = new byte[1024];
    private ExecutorService service;

    static {
        if (Config.logBuffWrites) {
            try {
                sessWriter = new PrintWriter("sessWriter.txt", "UTF-8");
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public BackgroundWorker(int id, NgCache cafe, String cachePoolName,
            String url, String user, String pass) {
        mc = new MemcachedClient(cachePoolName);
        this.id = id;
        this.cache = cafe;
        this.stats = Stats.getStatsInstance(id+100000);		

        try {
            conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);

            stmt = conn.createStatement();
        } catch (SQLException e) {
            System.out.println(String.format("Cannot make connection with url=%s, user=%s, pass=%s", url, user, pass));
            System.exit(-1);
        }

        service = Executors.newFixedThreadPool(3);
    }

    private int getNextIdx(int idx) {
        idx++;
        return idx < NUM_PENDING_WRITES_LOGS ? idx : 0;
    }

    @Override
    public void run() {
        System.out.println("Start background worker "+id);
        isRunning = true;

        Map<String, List<String>> its2sessIds = new HashMap<>();

        while (isRunning) {
            if (NgCache.policy != CachePolicy.WRITE_BACK) {
                try {
                    sleep(50);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                continue;
            }

            String teleW = Util.getEWLogKey(this.id);
            int hc = cache.cacheStore.getHashCode(teleW);

            List<String> sessIds = null;
            try {
                sessIds = Util.getSessionIdsFromKey(teleW, hc, null, mc, false);
            } catch (COException e2) {
                e2.printStackTrace(System.out);
            }

            if (sessIds == null || sessIds.size() == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace(System.out);
                }
                continue;
            }

            // Get a list of session ids here...
            List<String> toExec = new ArrayList<>();
            Map<Interval1D, List<String>> impactedIntervals = new HashMap<>();
            
            int checkpoint = 0;
            List<String> appliedSessIds = new ArrayList<>();
            for (String sessId: sessIds) {
                toExec.add(sessId);
                checkpoint++;
                appliedSessIds.add(sessId);

                if (toExec.size() == Config.BATCH) {
                    execBatchSession(toExec, its2sessIds, impactedIntervals, hc, teleW);
                    if (checkpoint == Config.BW_CHECK_POINT) {
                        updateMappingsAndTeleW(teleW, appliedSessIds, its2sessIds, impactedIntervals, hc);
                        checkpoint = 0;
                    }
                }

                if (!isRunning) {
                    break;
                }
            }

            if (toExec.size() > 0) {
                execBatchSession(toExec, its2sessIds, impactedIntervals, hc, teleW);
            }
            
            if (appliedSessIds.size() > 0) {
                updateMappingsAndTeleW(teleW, appliedSessIds, its2sessIds, impactedIntervals, hc);
                checkpoint = 0;
            }
        }

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (sessWriter != null)
            sessWriter.close();

        service.shutdown();

        System.out.println("Background worker stopped.");
    }

    private void updateMappingsAndTeleW(String teleW, List<String> appliedSessIds, Map<String, List<String>> its2sessIds,
            Map<Interval1D, List<String>> impactedIntervals, int hc) {
        // update teleW
        // RMW as a session
        updateTeleW(teleW, appliedSessIds, hc);

        // update the buffered-write key-value pairs
        // update each buffered write key-value pair using RMW as a session
        updateBufferedWriteKeyValuePairs(its2sessIds);
        
        // update the buffered writes in BuWrInT
        // for RangeQC
        updateBufferedWriteBuWrInT(impactedIntervals);
        
        // clear out the maps
        its2sessIds.clear();
        impactedIntervals.clear();
        appliedSessIds.clear();
    }

    private void updateBufferedWriteBuWrInT(
            Map<Interval1D, List<String>> impactedIntervals) {
        if (impactedIntervals.size() > 0) {
            String name = id+"";
            for (Interval1D interval: impactedIntervals.keySet()) {
                Dendrite d = cache.getDendrite(name);
                
                if (Config.persistMode == Config.NO_PERSIST) {
                    while (true) {
                        String sid = mc.generateSID();
                        Set<Integer> hcs = new HashSet<>();
                        Map<Integer, Integer> leases = null;
                        
                        try {
                            leases = d.acquireLeaseOnRange(interval.min, interval.max, IntervalLock.WRITE);
                            Set<Integer> pSet = d.getBufferIds(interval.min, interval.max);
                            List<String> sessIds = impactedIntervals.get(interval);    
                            
                            for (Integer p: pSet) {
                                if (pSet.size() > 1) {
                                    System.out.println("Found 2 keys in one interval. This should not happen.");
                                }
                                
                                String buffKey = String.format(Config.KEY_RANGE_ITEM_QUANTITY, name, p, p);
                                int hashCode = cache.cacheStore.getHashCode(buffKey);
                                hcs.add(hashCode);
                                
                                Object obj = mc.oqRead(sid, buffKey, hashCode, false);
                                if (obj != null) {
                                    String val = new String((byte[])obj);
                                    List<String> items = MemcachedHelper.convertList(val);
                                    int i = 0;
                                    while (i < items.size()) {
                                        String item = items.get(i);
                                        String id = item.split(",")[0];
                                        if (sessIds.contains(id)) {
                                            items.remove(item);
                                        } else {
                                            ++i;
                                        }
                                    }
            
                                    byte[] res = MemcachedHelper.convertString(items).getBytes();   
                                    
                                    Set<Integer> replicas = cache.getReplicaHashCodes(hashCode);
                                    if (Config.sendParallel) {
                                        int idx = 0;
                                        Future<Boolean> fs[] = new Future[Config.replicas];
                                        for (int r: replicas) {
                                            hcs.add(r);
                                            fs[idx++] = service.submit(new Callable<Boolean>() {
                                                @Override
                                                public Boolean call() throws Exception {
                                                    return mc.oqSwap(sid, buffKey, r, res, false);
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
                                        if (e != null) throw e;
                                    } else {
                                        for (int h : replicas) {
                                            mc.oqSwap(sid, buffKey, h, res, false);
                                            hcs.add(h);
                                        }
                                    }
                                }
                            }
                            
                            mc.dCommit(sid, hcs);
                            break;
                        } catch (Exception e) {
                            mc.dAbort(sid, hcs);
                        } finally {
                            if (leases != null) {
                                try {
                                    d.releaseLeaseOnRange(leases, interval.min, interval.max);
                                } catch (IOException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                } else {
                    Map<Integer, Integer> leases = null;
                    try {
                        leases = d.acquireLeaseOnRange(interval.min, interval.max, IntervalLock.WRITE);
                        Set<Integer> pSet = d.getBufferIds(interval.min, interval.max);
                        for (Integer p: pSet) {
                            String buffKey = String.format(Config.KEY_RANGE_ITEM_QUANTITY, name, p, p);
                            int hashCode = cache.cacheStore.getHashCode(buffKey);
                            Set<Integer> hcs = cache.getReplicaHashCodes(hashCode);
                            for (int hc: hcs) {
                                mc.deque(buffKey, this.id, 1, hc);
                            }
                        }
                    } catch (Exception e) {
                        
                    } finally {
                        if (leases != null) {
                            try {
                                d.releaseLeaseOnRange(leases, interval.min, interval.max);
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
    }

    private void execBatchSession(List<String> toExec, 
            Map<String, List<String>> its2sessIds, Map<Interval1D, List<String>> impactedIntervals, int hc, String teleW) {        
        // execute sessions
        // as one session
        exec(toExec, its2sessIds, hc, impactedIntervals, sessWriter);
        stats.incrBy("applied_sessions", toExec.size());

        // delete Session key-value pairs
        // don't count this for diff
        Set<Integer> hcs = cache.getReplicaHashCodes(hc);
        for (String id: toExec) {
            String key = String.format("S-%s", id);
            
            for (int i: hcs) {
                try {
                    mc.delete(key, i, null);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        // clean up the CommitedSession table
        cache.cacheBack.cleanupSessionTable(toExec);
        
        toExec.clear();
    }

    private void updateTeleW(String teleW, List<String> appliedSessIds, int hashCode) {
        if (appliedSessIds.size() > 0) {
            if (Config.persistMode == Config.NO_PERSIST || Config.persistMode == Config.BDB_ASYNC) {
                Util.updateSessionIdsToKey(teleW, appliedSessIds, mc, 
                        hashCode, stats, service, cache, id);
            } else {
                Set<Integer> hcs = cache.getReplicaHashCodes(hashCode);
                if (Config.sendParallel) {
                    Future<Boolean> fs[] = new Future[Config.replicas];
                    int idx = 0;
                    for (final int h: hcs) {
                        fs[idx++] = service.submit(new Callable<Boolean>() {
                            @Override
                            public Boolean call() throws Exception {
                                try {
                                    mc.deque(teleW, id, appliedSessIds.size(), h);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                                return true;
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
                    for (int h: hcs) {
                        try {
                            mc.deque(teleW, this.id, appliedSessIds.size(), h);
                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private void updateBufferedWriteKeyValuePairs(Map<String, List<String>> its2sessIds) {
        for (String it: its2sessIds.keySet()) {
            String buffKey = Util.getBufferedWriteKey(it);

            if (Config.persistMode == Config.NO_PERSIST) {
                String indexesKey = Util.getIndexesKey(buffKey);
                int hashCode = cache.cacheStore.getHashCode(buffKey);
    
                Set<Integer> hcs = cache.getReplicaHashCodes(hashCode);
                Set<Integer> addedHashCodes = new HashSet<>();
    
                while (true) {
                    String sid = mc.generateSID();
                    addedHashCodes.clear();
    
                    try {
                        addedHashCodes.add(hashCode);
                        Object bwIndexesVal = mc.oqRead(sid, indexesKey, hashCode, true);
                        int min = 0, max = 0;
                        if (bwIndexesVal != null) {                        
                            String[] minmax = ((String)bwIndexesVal).split(",");
                            min = Integer.parseInt(minmax[0]);
                            max = Integer.parseInt(minmax[1]);
                        }                    
    
                        List<String> applied_sess_ids = its2sessIds.get(it);
                        int diff = 0;
                        if (applied_sess_ids != null) {
                            boolean removeIndexes = false;
    
                            for (int i = min; i <= max; ++i) {
                                if (applied_sess_ids.size() == 0)
                                    break;
    
                                String bwArchiveKey = Util.getBufferdWriteArchivePartitionKey(buffKey, i);
                                Object obj = mc.oqRead(sid, bwArchiveKey, hashCode, false);
                                
                                List<String> session_ids = null;
                                if (obj != null) {
                                    String val = new String((byte[])obj);
                                    session_ids = MemcachedHelper.convertList(val);
                                    Iterator<String> iter = applied_sess_ids.iterator();
                                    while (iter.hasNext()) {
                                        String sess_id = iter.next();
                                        if (session_ids.remove(sess_id)) {
                                            iter.remove();
                                            diff += (sess_id.length() * Config.replicas);
                                            if (applied_sess_ids.size() == 0) {
                                                diff += (bwArchiveKey.length() * Config.replicas);
                                                break;
                                            }
                                        }
                                    }
    
                                    String s = MemcachedHelper.convertString(session_ids);
                                    final byte[] o = (s != null) ? s.getBytes() : null;
                                    if (Config.sendParallel) {
                                        Future<Boolean> fs[] = new Future[Config.replicas];
                                        int idx = 0;
                                        for (final int h: hcs) {
                                            addedHashCodes.add(h);
                                            fs[idx++] = service.submit(new Callable<Boolean>() {
                                                @Override
                                                public Boolean call() throws Exception {
                                                    return mc.oqSwap(sid, bwArchiveKey, h, o, false);
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
                                        if (e != null) throw e;
                                    } else {
                                        for (int r: hcs) {
                                            mc.oqSwap(sid, bwArchiveKey, r, o, false);
                                            addedHashCodes.add(r);
                                        }
                                    }
                                }
    
                                if (session_ids == null || session_ids.size() == 0) {
                                    min = i + 1;
                                    if (min > max) {
                                        removeIndexes = true;
                                    }
                                }
                            }
                            
                            final String val = removeIndexes ? null : min + "," + max;
    
                            if (Config.sendParallel) {
                                Future<Boolean> fs[] = new Future[Config.replicas];
                                int idx = 0;
                                for (int r : hcs) {
                                    addedHashCodes.add(r);
                                    fs[idx++] = service.submit(new Callable<Boolean>() {
                                        @Override
                                        public Boolean call() throws Exception {
                                            return mc.oqSwap(sid, indexesKey, r, val, true);
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
                                if (e != null) throw e;
                            } else {
                                for (int r : hcs) {
                                    mc.oqSwap(sid, indexesKey, r, val, true);
                                    addedHashCodes.add(r);
                                }
                            }
    
                            if (applied_sess_ids.size() > 0) {
                                Object obj = mc.oqRead(sid, buffKey, hashCode, false);
                                if (obj != null) {
                                    String v = new String((byte[])obj);
                                    List<String> session_ids = MemcachedHelper.convertList(v);
                                    Iterator<String> iter = applied_sess_ids.iterator();
    
                                    while (iter.hasNext()) {
                                        String sess_id = iter.next();
                                        if (session_ids.remove(sess_id)) {
                                            diff += (sess_id.length() * Config.replicas);
                                            iter.remove();
                                            if (applied_sess_ids.size() == 0) {
                                                diff += (buffKey.length() * Config.replicas);
                                                break;
                                            }
                                        }
                                    }
    
                                    assert (applied_sess_ids.size() == 0);
    
                                    byte[] sobj = null;
                                    String s = MemcachedHelper.convertString(session_ids);
                                    if (s != null) sobj = s.getBytes();
                                    final Object o = sobj;
                                    if (Config.sendParallel) {
                                        Future<Boolean> fs[] = new Future[Config.replicas];
                                        int idx = 0;
                                        for (int hc: hcs) {
                                            addedHashCodes.add(hc);
                                            fs[idx++] = service.submit(new Callable<Boolean>() {
                                                @Override
                                                public Boolean call() throws Exception {
                                                    return mc.oqSwap(sid, buffKey, hc, o, false);
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
                                        if (e != null) throw e;
                                    } else {
                                        for (int r : hcs) {
                                            mc.oqSwap(sid, buffKey, r, o, false);
                                            addedHashCodes.add(r);
                                        }
                                    }
                                }
                            }
                        }
    
                        mc.dCommit(sid, addedHashCodes);
                        if (stats != null) {
                            stats.decrBy(Stats.METRIC_BW_SIZE, diff);
                        }
    
                        break;
                    } catch (COException e) {
                        try {
                            mc.dAbort(sid, addedHashCodes);
                        } catch (Exception e1) {
                            // TODO Auto-generated catch block
                            //                        e1.printStackTrace();
                        }
                    } catch (Exception e) {
                        try {
                            mc.dAbort(sid, addedHashCodes);
                        } catch (Exception e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                        //                    e.printStackTrace(System.out);
                    }
    
                    try {
                        sleep(50);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            } else {
                int id = cache.cacheStore.getHashCode(buffKey);
                Set<Integer> hcs = cache.getReplicaHashCodes(id);
                for (int hc: hcs) {
                    try {
                        mc.deque(buffKey, this.id, its2sessIds.get(it).size(), hc);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void exec(List<String> toExec, Map<String, List<String>> its2sessIds, 
            int hc, Map<Interval1D, List<String>> impactedIntervals, PrintWriter sessionWriter) {
        String[] keys = new String[toExec.size()];

        for (int i = 0; i < keys.length; ++i) {
            keys[i] = String.format("S-%s", toExec.get(i));
        }

        executeWithLease(hc, keys, toExec, impactedIntervals, its2sessIds, sessionWriter);
    }

    private void executeWithLease(int hc, String[] keys, List<String> toExec,
            Map<Interval1D, List<String>> impactedIntervals, 
            Map<String, List<String>> its2sessIds, PrintWriter sessionWriter) {

        Set<Integer> addedHashCodes = new TreeSet<>();
        List<Session> sessList = new ArrayList<>();
        
        while (true) {
            String sid = mc.generateSID();
            int diff = 0;
            impactedIntervals.clear();
            addedHashCodes.clear();
            sessList.clear();

            try {
                Map<String, Object> res = mc.co_getMulti(sid, hc, 1, false, keys);
                addedHashCodes.add(hc);

                List<String> existed = cache.cacheBack.checkExists(toExec);


                for (int i = 0; i < keys.length; ++i) {
                    Object val = res.get(keys[i]);
                    if (val != null) {
                        if (existed != null && existed.contains(toExec.get(i)))
                            continue;

                        diff += (keys[i].length() + ((byte[])val).length) * Config.replicas;

                        Map<String, List<Change>> changesMap = cache.cacheBack.deserializeSessionChanges((byte[])val);
                        if (changesMap != null && changesMap.size() > 0) {
                            Session sess = new Session(toExec.get(i));
                            for (String it: changesMap.keySet()) {
                                List<Change> changes = changesMap.get(it);
                                for (Change c: changes) {
                                    sess.addChange(it, c);
                                }
                            }
                            sessList.add(sess);
                        }
                    } else {
                        System.out.println("Buffered write is null key="+keys[i]+". This should not happen in normal mode.");
                    }
                }

                System.out.println("BGT executes "+ sessList.size()+ " sessions to data store.");
                cache.cacheBack.applySessions(sessList, conn, stmt, sessionWriter, stats);

                Object[] vals = new Object[keys.length];
                for (int i = 0; i < vals.length; ++i) 
                    vals[i] = null;

                Set<Integer> hcs = cache.getReplicaHashCodes(hc);
                if (Config.sendParallel) {
                    Future<Map<String, Boolean>> fs[] = new Future[Config.replicas];                    
                    int idx = 0;
                    for (int i: hcs) {
                        addedHashCodes.add(i);
                        fs[idx++] = service.submit(new Callable<Map<String, Boolean>>() {
                            @Override
                            public Map<String, Boolean> call() throws Exception {
                                return mc.oqSwaps(sid, keys, vals, i, false);
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
                    if (e != null) throw e;
                } else {
                    for (Integer i: hcs) {
                        mc.oqSwaps(sid, keys, vals, i, false);
                        addedHashCodes.add(i);
                    }
                }

                conn.commit();
                mc.dCommit(sid, addedHashCodes);

                if (stats != null) {
                    stats.decrBy(Stats.METRIC_BW_SIZE, diff);
                    stats.incrBy("exec_sessions", sessList.size());
                    stats.incr(Stats.METRIC_AR_COMMIT_SESSONS);
                }

                for (Session sess: sessList) {
                    List<String> its = sess.getIdentifiers();
                    for (String it: its) {
                        List<String> sessIds = its2sessIds.get(it);
                        if (sessIds == null) {
                            sessIds = new ArrayList<>();
                            its2sessIds.put(it, sessIds);
                        }
                        sessIds.add(sess.getSid());
                    }
                }

                break;
            } catch (Exception e) {  
                e.printStackTrace(System.out);
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    //                    e1.printStackTrace(System.out);
                }

                try {
                    mc.dAbort(sid, addedHashCodes);
                    stats.incr(Stats.METRIC_AR_ABORT_SESSIONS);
                } catch (Exception e1) {
                    //                    e1.printStackTrace(System.out);
                }
            }

            try {
                Thread.sleep(Config.AR_SLEEP);
            } catch (InterruptedException e) {
                e.printStackTrace(System.out);
            }
        }
        
        // for RangeQC
        for (Session sess: sessList) {
            Map<Interval1D, String> impacted = cache.cacheBack.getImpactedRanges(sess);
            for (Interval1D interval: impacted.keySet()) {
                List<String> sessions = impactedIntervals.get(interval);
                if (sessions == null) {
                    sessions = new ArrayList<>();
                    impactedIntervals.put(interval, sessions);
                }
                sessions.add(impacted.get(interval));
            }
        }
    }

    private Set<?> pickRandom(Set<? extends Object> idset, int alpha) {
        if (alpha >= idset.size())
            return idset;

        BitSet bs = new BitSet(idset.size());
        int cardinality = 0;
        while(cardinality < alpha) {
            int v = rand.nextInt(alpha);
            if(!bs.get(v)) {
                bs.set(v);
                cardinality++;
            }
        }

        Set<Object> chosen = new HashSet<>();
        int i = 0;
        for (Object id: idset) {
            if (bs.get(i++))
                chosen.add(id);
        }

        return chosen;
    }

//    private int updateEW(String ewKey, Set<String> recoverSet) {
//        Object obj = null;
//        List<String> idlist = null;
//        int hc = getHashCode(ewKey);
//
//        while (true) {
//            String sid = mc.generateSID();
//
//            try {
//                obj = mc.oqRead(sid, ewKey, hc, true);
//
//                if (obj != null) {
//                    if (obj != null) {
//                        idlist = convertList(obj);
//                        for (String it: recoverSet)
//                            idlist.remove(it);
//                        String newVal = convertString(idlist);
//                        mc.oqSwap(sid, ewKey, hc, newVal);
//                    }
//                }
//
//                mc.validate(sid, hc);
//                mc.dCommit(sid, hc);
//                break;
//            } catch (COException | IOException e1) {
//                try {
//                    mc.dAbort(sid, hc);
//                } catch (Exception e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//
//            try {
//                Thread.sleep(LEASE_BACKOFF_TIME);
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
//
//        return idlist == null ? 0 : idlist.size();
//    }

//    private int getHashCode(String key) {
//        return Integer.parseInt(key.replaceAll("[^0-9]", ""));
//    }

    public void setRunning(boolean b) {
        this.isRunning = b;
    }
}