package com.usc.dblab.cafe;

import static com.usc.dblab.cafe.SessionProfile.METRIC_CIGET;
import static com.usc.dblab.cafe.SessionProfile.METRIC_COGETS_C;
import static com.usc.dblab.cafe.SessionProfile.METRIC_COGETS_O;
import static com.usc.dblab.cafe.SessionProfile.METRIC_DMLS;
import static com.usc.dblab.cafe.SessionProfile.METRIC_IQSET;
import static com.usc.dblab.cafe.SessionProfile.METRIC_KEYS_IN_COGETS_C;
import static com.usc.dblab.cafe.SessionProfile.METRIC_KEYS_IN_COGETS_O;
import static com.usc.dblab.cafe.SessionProfile.METRIC_KEYS_IN_OQAPPENDS;
import static com.usc.dblab.cafe.SessionProfile.METRIC_KEYS_IN_OQSWAPS;
import static com.usc.dblab.cafe.SessionProfile.METRIC_OQAPPEND;
import static com.usc.dblab.cafe.SessionProfile.METRIC_OQAPPENDS;
import static com.usc.dblab.cafe.SessionProfile.METRIC_OQDECR;
import static com.usc.dblab.cafe.SessionProfile.METRIC_OQINCR;
import static com.usc.dblab.cafe.SessionProfile.METRIC_OQSWAPS;
import static com.usc.dblab.cafe.SessionProfile.METRIC_OQWRITE;
import static com.usc.dblab.cafe.SessionProfile.METRIC_QUERIES;
import static com.usc.dblab.cafe.SessionProfile.METRIC_SESSION_ABORTS;
import static com.usc.dblab.cafe.SessionProfile.METRIC_SESSION_COMMITS;
import static com.usc.dblab.cafe.SessionProfile.METRIC_SESSION_STARTS;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import com.meetup.memcached.COException;
import com.meetup.memcached.IQException;
import com.meetup.memcached.MemcachedClient;
import com.usc.dblab.ngcache.dendrite.BufferedWriteItem;
import com.usc.dblab.ngcache.dendrite.Dendrite;

import edu.usc.dblab.intervallock.IntervalLock;
import edu.usc.dblab.intervaltree.Interval1D;
import edu.usc.dblab.intervaltree.IntervalData;
import edu.usc.dblab.itcomp.client.KeyHashCode;

/**
 * Each thread must instantiate one instance of this class.
 * Cache Augmented Framework Element.
 * @author hieun
 *
 */
public class NgCache {
    private static final String BW_RESERVED_STR = "xxxx";

    private static PrintWriter writer = null;

    static final int NUM_PARTITIONS_IT = 1;

    final static ConcurrentHashMap<String, Dendrite> dendrites = new ConcurrentHashMap<>();
    final static Semaphore dendriteSemaphore = new Semaphore(1);

    final CacheStore cacheStore;
    final WriteBack cacheBack;

    int diff = 0;
    int skv_diff = 0;

    private final MemcachedClient mc;
    private final byte[] buffer;
    public static volatile CachePolicy policy;
    private final Stats stats;
    
    private final SessionProfile profile;
    private CachePolicy mode;

    final MemcachedClient[] mcClients;
    
    private String cachePoolName;

    // pending session variables
    // must clean up after committing or aborting a session.
    private String sessId = null;		// buffer for storing the id of pending session.    
    final private Map<String, Integer> keysToHash = new HashMap<>();
    final private Map<Integer, Set<String>> hashToKeys = new TreeMap<>();
    final private Set<Integer> addedHashCodes = new TreeSet<>();

    // Use of tree map to make them sorted by the keys.
    // Leases will be acquired based on this order to make sessions detect conflict as soon as it can.
    private final TreeMap<String, List<Delta>> sessionDeltas;
    private final TreeMap<String, List<Change>> sessionChanges;
    
    private final Map<String, Map<Interval1D, List<Delta>>> pointDeltas;
    private final Map<String, Map<Interval1D, List<Change>>> pointChanges;
    
    private Dendrite activeDendrite = null;
    private Interval1D interval = null;
    private Map<Integer, Integer> pendingLeases = null;

    private String name;
    private String tag = null;

    private final static Logger logger = Logger.getLogger(NgCache.class);

    private static PrintWriter bwWriter = null;

    static {
        if (false && Config.logBuffWrites) {
            try {
                bwWriter = new PrintWriter("bwWriter.txt", "UTF-8");
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                bwWriter = null;
            }

            try {
                writer = new PrintWriter("dmllog.txt");
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            bwWriter = null;
            writer = null;
        }
    }

    /**
     * Initialize the Cafe.
     * @param cacheStore
     * @param cacheBack
     * @param cachePoolName
     * @param policy
     * @param numBackgroundWorkers 
     * @param benchmarkModule 
     */
    public NgCache(CacheStore cacheStore, WriteBack cacheBack, String cachePoolName, 
            CachePolicy policy, int numBackgroundWorkers, Stats stats, 
            String url, String user, String pass, boolean profile, int arsleep, int minId, int maxId) {
        this.cacheStore = cacheStore;
        this.cacheBack = cacheBack;
        
        this.sessionDeltas = new TreeMap<>();
        this.sessionChanges = new TreeMap<>();
        this.pointDeltas = new HashMap<>();
        this.pointChanges = new HashMap<>();
        
        this.profile = new SessionProfile(profile);        

        this.buffer = new byte[1024*5];
        this.mc = new MemcachedClient(cachePoolName);
        this.cachePoolName = cachePoolName;

        this.policy = policy;
        this.stats = stats;        

        Config.AR_SLEEP = arsleep;
        
        System.out.println("Cafe version 3");

        if (Config.storeCommitedSessions && this.cacheBack != null)
            this.cacheBack.createSessionTable();

        // init the background worker threads
        BackgroundService.init(this, cachePoolName, numBackgroundWorkers, url, user, pass, minId, maxId);

        mcClients = new MemcachedClient[Config.replicas];
        for (int i = 0; i < Config.replicas; ++i) {
            mcClients[i] = new MemcachedClient(cachePoolName);
        }
    }
    
    public void setPendingRangeLease(Dendrite activeDendrite, 
            Interval1D interval, int mode) throws IOException {
        if (this.activeDendrite != null) {
            System.out.println("Something went wrong. Just try to release the lease here.");
            clearPendingRangeLease();
        }
        
        Map<Integer, Integer> leases = activeDendrite.acquireLeaseOnRange(interval.min, interval.max, mode);
        this.activeDendrite = activeDendrite;
        this.interval = interval;
        this.pendingLeases = leases;
    }
    
    public void clearPendingRangeLease() {
        if (this.activeDendrite != null) {
            try {
                this.activeDendrite.releaseLeaseOnRange(pendingLeases, interval.min, interval.max);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            this.activeDendrite = null;
            this.interval = null;
            this.pendingLeases = null;
        }
    }

    public Stats getStats() {
        return stats;
    }
    
    public void startSession(String name, String tag) throws SessionExistsException {
        startSession(name);
        this.sessId = String.format("%s-%s", tag, sessId);
        this.tag = tag;
    }

    /**
     * Start a session. Internally, a session id is created and maintained by the object.
     * @throws SessionExistsException
     */
    public void startSession(String name) throws SessionExistsException {
        if (sessId != null) {
            throw new SessionExistsException("Old session has not been committed or aborted.");
        }
        sessId = UUID.randomUUID().toString();
//        seqId = 0;        
        profile.init(name);
        profile.incr(METRIC_SESSION_STARTS);
        mode = policy;
        this.name = name;

        if (bwWriter != null) {
            bwWriter.println(name);
        }

        if (writer != null) {
            writer.println(name);
        }
    }

    public boolean validateSession() throws Exception {
        if (mode == CachePolicy.WRITE_THROUGH || mode == CachePolicy.WRITE_BACK) {
            applyDeltas(sessId, sessionDeltas);
        }
        
        // validate the session first
        boolean success = mc.validate(sessId, hashToKeys.keySet());
            
        if (!success)
            return false;

        // apply buffered write to the cache 
        // append the session id to mapping keys.
        if (mode == CachePolicy.WRITE_BACK) {
            diff += bufferChanges(sessId, sessionChanges);
        }

        // append the session ids to session queue (TeleW)
        if (mode == CachePolicy.WRITE_BACK) {
            if (sessionChanges != null && sessionChanges.size() > 0) {
                int p = cacheBack.getTeleWPartition(sessId);
                String teleW =  Util.getEWLogKey(p);
                int hc = getHashCode(teleW);

                Set<Integer> hcs = getReplicaHashCodes(hc);
                if (Config.sendParallel) {
                    Future<Integer>[] fs = new Future[Config.replicas];
                    int idx = 0;
                    for (final int i: hcs) {
                        addedHashCodes.add(i);
                        Future<Integer> f = BackgroundService.getService().submit(new Callable<Integer>() {
                            @Override
                            public Integer call() throws Exception {
                                return Util.appendOrAddSessId(teleW, i, sessId, (Config.DELIMITER+sessId).getBytes(), mc, true);
                            }
                        });
                        fs[idx++] = f;
                    }

                    Exception e = null;
                    for (Future<Integer> f: fs) {
                        try {
                            diff += f.get().intValue();
                        } catch (Exception ex) {
                            e = ex;
                        }
                    }
                    if (e != null) throw e;
                } else {
                    for (final int i: hcs) {
                        addedHashCodes.add(i);
                        diff += Util.appendOrAddSessId(teleW, i, sessId, (Config.DELIMITER+sessId).getBytes(), mc, true);
                    }
                }
            }
        }

        return success;
    }

    /**
     * Commits a session. This includes invoking the validate command to validate the cache state.
     * If validation succeeds, the session commits.
     * Otherwise, it aborts.
     * @return
     * @throws Exception
     */
    public boolean commitSession() {
        logger.debug("Commit session "+sessId);
        for (int hashCode: hashToKeys.keySet()) {
            logger.debug("Hash Code "+hashCode+" "+ Arrays.toString(hashToKeys.keySet().toArray(new Integer[0])));
        }

        addedHashCodes.addAll(hashToKeys.keySet());
        boolean success = mc.dCommit(sessId, addedHashCodes);
        
        if (stats != null) {
            stats.incr(Stats.METRIC_COMMITED_SESSIONS);
            stats.incrBy(Stats.METRIC_BW_SIZE, diff);
            stats.incrBy(Stats.METRIC_BW_SKV_SIZE, skv_diff);
        }

        profile.incr(METRIC_SESSION_COMMITS);

        cleanSession();

        return success;
    } 

    private void cleanSession() {
        hashToKeys.clear();
        keysToHash.clear();
        addedHashCodes.clear();
        
        sessId = null;
        tag = null;

        // normal changes
        sessionChanges.clear();
        sessionDeltas.clear();
        
        // CPR changes
        pointDeltas.clear();
        pointChanges.clear();
        
        clearPendingRangeLease();
        
        diff = 0;
        skv_diff = 0;
    }

    /**
     * Aborts the session.
     * @return
     * @throws Exception
     */
    public boolean abortSession() {
        logger.debug("Abort session "+sessId);
        for (int hashCode: hashToKeys.keySet()) {
            logger.debug("Hash Code "+hashCode+" "+ Arrays.toString(hashToKeys.get(hashCode).toArray(new String[0])));
        }

        addedHashCodes.addAll(hashToKeys.keySet());
        boolean success =  mc.dAbort(sessId, addedHashCodes);

        
        cleanSession();
        if (stats != null) {
            stats.incr(Stats.METRIC_ABORTED_SESSIONS);
        }        
        profile.incr(METRIC_SESSION_ABORTS);

        return success;
    }


    // sample query: QUERY_STOCK_LEVEL,1,10,45
    public QueryResult rangeReadStatement(String query) throws Exception {
        if (stats != null) stats.incr(Stats.METRIC_READ_STATEMENTS);
        
        // identify the warehouse id w
        String name = cacheStore.getCollectionName(query);
        Dendrite dendrite = getDendrite(name);
        
        Interval1D interval = cacheStore.getBounds(query);
        
        Set<String> keys = new TreeSet<>();
        BitSet bs = new BitSet(interval.max-interval.min+1);
        setPendingRangeLease(dendrite, interval, IntervalLock.READ);
        
        List<IntervalData> intervals = dendrite.overlappers(interval.min, interval.max);
        for (IntervalData i: intervals) {
            keys.addAll(i.getKeys());
            for (int p = i.getLower(); p <= i.getUpper(); ++p) {
                if (p >= interval.min && p <= interval.max) {
                    bs.set(p - interval.min);
                }
            }
        }
        clearPendingRangeLease();
       
        Set<CacheEntry> entries = new HashSet<>();
        if (keys.size() > 0) {
            String[] keysArr = keys.toArray(new String[0]);
            Arrays.sort(keysArr);
            int hc = getHashCode(keysArr[0]);

            // do a multiget
            Map<String, Object> objs = mc.co_getMulti(sessId, hc, 0, false, keysArr);
            for (int i = 0; i < keysArr.length; ++i) {
                String key = keysArr[i];
                Object obj = objs.get(key);
                if (obj != null) {
                    CacheEntry entry = cacheStore.deserialize(key, obj, buffer);
                    entries.add(entry);
                } else {
//                    System.out.println("Get null value in rangeQuery for key "+key);
                }
            }
        }
        
        // all cache hits
        if (bs.cardinality() == interval.max - interval.min + 1) {
            return cacheStore.computeQueryResult(query, entries);
        }

        // handle missingRanges?
        for (int i = 0; i < interval.max-interval.min+1; ++i) {
            if (!bs.get(i)) {
                int quantity = i+interval.min;
                String newQuery = cacheStore.constructFixPointQuery(query, quantity);
                Set<String> newKeys = cacheStore.getReferencedKeysFromQuery(newQuery);
                String[] keysArr = newKeys.toArray(new String[0]);
                int hc = getHashCode(keysArr[0]);
                
                setPendingRangeLease(dendrite, new Interval1D(quantity, quantity), IntervalLock.READ);
                
                Map<String, Object> objsMap = mc.co_getMulti(sessId, hc, 0, false, keysArr);
                
                if (objsMap != null && objsMap.size() > 0) {
                    for (String k: objsMap.keySet()) {
                        newKeys.remove(k);
                    }
                }
                
                // handle any cache miss
                if (newKeys.size() > 0) {
                    QueryResult result = cacheStore.queryDataStore(newQuery);
                    
                    if (mode == CachePolicy.WRITE_BACK) {
                        // query any pending buffered writes
                        Map<Integer, List<BufferedWriteItem>> items = dendrite.getBufferedWrite(quantity, quantity);
                        
                        // merge items into result
                        cacheBack.merge(newQuery, result, items);
                    }
                    
                    Set<CacheEntry> ets = cacheStore.computeCacheEntries(newQuery, result);
                    for (CacheEntry et: ets) {
                        String key = et.getKey();
                        if (newKeys.contains(key)) {
                            byte[] bytes = cacheStore.serialize(et);
                            mc.iqset(key, bytes, hc);
                            dendrite.insertInterval(quantity, quantity, key);
                            System.out.println(String.format("Query %s inserts key %s, interval [%d, %d] to Dendrite %s", 
                                    query, key, quantity, quantity, name));
                            newKeys.remove(key);
                        }
                    }
                    entries.addAll(ets);
                }
                
                clearPendingRangeLease();
                
                if (newKeys.size() > 0) {
                    System.out.println("Still have missing key-value pairs? "+Arrays.toString(newKeys.toArray(new String[0])));
                }
            }
        }

        // finally, compute the final result and return
        return cacheStore.computeQueryResult(query, entries);
    }

    Dendrite getDendrite(String name) {
        Dendrite dendrite = dendrites.get(name);
        if (dendrite == null) {
            try {
                dendriteSemaphore.acquire();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            dendrite = dendrites.get(name);
            if (dendrite == null) {
                KeyHashCode hc = new CustomKeyHashCode(this);
                
                InetAddress addr = null;
                try {
                    addr = getLocalHostLANAddress();
                } catch (UnknownHostException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
                int port = 13371;
                port = port + Integer.parseInt(name);
                System.out.println("Port is "+port);
                if (Config.sendParallel) {
                    dendrite = new Dendrite(name, 0, 100, 1, 1, new String[] { addr.getHostAddress()+":"+port }, true, 
                            cachePoolName, null, hc, true, true, false, (Config.replicas > 1), BackgroundService.getService());
                } else {
                    dendrite = new Dendrite(name, 0, 100, 1, 1, new String[] { addr.getHostAddress()+":"+port }, true, 
                            cachePoolName, null, hc, true, true, false, (Config.replicas > 1), null);
                }
                dendrites.put(name, dendrite);
            }
            
            dendriteSemaphore.release();
        }
        
//        String s = "Got Dendrite:";
//        s = String.format("%s myClientStr %s,", s, dendrite.myClientstr);
//        s = String.format("%s myIndex %s,", s, dendrite.myIndex);
//        s = String.format("%s - name=%s", s, name);
//        System.out.println(s);
        
        return dendrite;
    }
    
    private static InetAddress getLocalHostLANAddress() throws UnknownHostException {
        try {
            InetAddress candidateAddress = null;
            // Iterate all NICs (network interface cards)...
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                // Iterate all IP addresses assigned to each card...
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {

                        if (inetAddr.isSiteLocalAddress()) {
                            // Found non-loopback site-local address. Return it immediately...
                            return inetAddr;
                        }
                        else if (candidateAddress == null) {
                            // Found non-loopback address, but not necessarily site-local.
                            // Store it as a candidate to be returned if site-local address is not subsequently found...
                            candidateAddress = inetAddr;
                            // Note that we don't repeatedly assign non-loopback non-site-local addresses as candidates,
                            // only the first. For subsequent iterations, candidate will be non-null.
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                // We did not find a site-local address, but we found some other non-loopback address.
                // Server might have a non-site-local address assigned to its NIC (or it might be running
                // IPv6 which deprecates the "site-local" concept).
                // Return this non-loopback candidate address...
                return candidateAddress;
            }
            // At this point, we did not find a non-loopback address.
            // Fall back to returning whatever InetAddress.getLocalHost() returns...
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            if (jdkSuppliedAddress == null) {
                throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
            }
            return jdkSuppliedAddress;
        }
        catch (Exception e) {
            UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }

    public QueryResult readStatement(String query) throws Exception {
        if (stats != null)
            stats.incr(Stats.METRIC_READ_STATEMENTS);

        long time = System.currentTimeMillis();
        String pre = query.split(",")[0];

        Set<String> keys = cacheStore.getReferencedKeysFromQuery(query);
        Set<CacheEntry> entries = new HashSet<>(); 

        boolean miss = false;
        Set<String> missKeys = new HashSet<>();
        if (keys == null || keys.size() == 0) {
            miss = true;
        } else {
            for (String key: keys) {
                int hc = getHashCode(key);

                CacheEntry ce = null;
                Object val = mc.ciget(sessId, key, false, hc);

                profile.incr(METRIC_CIGET);

                if (val == null) {
                    miss = true;
                    missKeys.add(key);
                } else {
                    if (val instanceof String) {
                        ce = new CacheEntry(key, (String)val, true);
                    } else {
                        ce = cacheStore.deserialize(key, (byte[])val, buffer);
                    }
                    entries.add(ce);
                }
            }
        }

        // all cache hits
        if (!miss) {
            if (stats != null)
                stats.incrBy(Stats.METRIC_CACHE_HITS, keys.size());
            QueryResult result = cacheStore.computeQueryResult(query, entries);
            return result;
        }

        if (stats != null) {
            if (missKeys.size() > 0) {
                stats.incrBy(Stats.METRIC_CACHE_MISSES, missKeys.size());
                if (mode == CachePolicy.WRITE_BACK) {
                    System.out.println("Missed keys "+name+" "+Arrays.toString(missKeys.toArray(new String[0])));
                }
            } else {
                stats.incr(Stats.METRIC_QUERY_NO_CACHE);
            }
        }

        // query the data store
        QueryResult result = cacheStore.queryDataStore(query);
        profile.incr(METRIC_QUERIES);

        // at least one cache miss.
        if (mode == CachePolicy.WRITE_BACK) {
            Set<String> its = cacheBack.rationalizeRead(query);

            Map<Integer, List<String>> keyMap = new HashMap<>();
            Map<String, String> indexesKeyToBuffKey = new HashMap<>();

            // get the impacted sessions
            int hc = 0;
            for (String it: its) {
                String buffKey = Util.getBufferedWriteKey(it);
                String indexesKey = Util.getIndexesKey(buffKey);
                hc = getHashCode(indexesKey);
                List<String> list = keyMap.get(hc);
                if (list == null) {
                    list = new ArrayList<>();
                    keyMap.put(hc, list);
                }
                list.add(indexesKey);
                indexesKeyToBuffKey.put(indexesKey, buffKey);
            }

            List<String> sessIds = new ArrayList<>();

            // go to each server
            for (int hashCode: keyMap.keySet()) {
                List<String> keyList = keyMap.get(hc);
                Map<String, Object> res = mc.co_getMulti(sessId, hashCode, 1, true, keyList.toArray(new String[0]));
                for (String key: keyList) {
                    Object bwIndexesVal = res.get(key);
                    int min = 0, max = 0;
                    if (bwIndexesVal != null) {                        
                        String[] minmax = ((String)bwIndexesVal).split(",");
                        min = Integer.parseInt(minmax[0]);
                        max = Integer.parseInt(minmax[1]);
                    }

                    String[] ks = new String[max-min+1+1];
                    String buffKey = indexesKeyToBuffKey.get(key);
                    for (int i = min; i <= max; ++i) {
                        ks[max-min] = Util.getBufferdWriteArchivePartitionKey(buffKey, i);
                    }
                    ks[ks.length-1] = indexesKeyToBuffKey.get(key);

                    Map<String, Object> res2 = mc.co_getMulti(sessId, hc, 1, true, ks);
                    for (int i = 0; i < ks.length; ++i) {
                        Object val = res2.get(ks[i]);
                        if (val != null) {
                            sessIds.addAll(MemcachedHelper.convertList((String)val));
                        }
                    }
                }
            }

            // we are assuming there is just one hashCode here
            // TODO: must be fixed if the impacted buffered write key-value pairs are on multiple cache servers.
            int cnt = 0;
            int b = 20;
            List<Session> sessList = new ArrayList<>();
            while (cnt < sessIds.size()) {
                int p = b;
                if (sessIds.size()-cnt < b) p = sessIds.size()-cnt;

                String[] sessKeys = new String[p];
                for (int i = 0; i < p; ++i) {
                    sessKeys[i] = String.format("S-%s", sessIds.get(cnt+i));
                }

                Map<String, Object> res = mc.co_getMulti(sessId, hc, 1, false, sessKeys);
                for (int i = 0; i < sessKeys.length; ++i) {
                    Object val = res.get(sessKeys[i]);
                    if (val != null) {
                        Map<String, List<Change>> changesMap = cacheBack.deserializeSessionChanges((byte[])val);
                        if (changesMap != null && changesMap.size() > 0) {
                            Session sess = new Session(sessIds.get(i));
                            for (String it: changesMap.keySet()) {
                                List<Change> changes = changesMap.get(it);
                                for (Change c: changes) {
                                    sess.addChange(it, c);
                                }
                            }
                            sessList.add(sess);
                        }
                    }
                }

                cnt += p;
            }

            LinkedHashMap<String, List<Change>> changes = new LinkedHashMap<>();
            for (Session s: sessList) {
                for (String it: its) {
                    int idx = s.getIdentifiers().indexOf(it);
                    if (idx != -1) {
                        Change c = s.getChanges().get(idx);
                        List<Change> cs = changes.get(it);
                        if (cs == null) {
                            cs = new ArrayList<>();
                            changes.put(it, cs);
                        }
                        cs.add(c);
                    }
                }
            }

            // combine with the local one
            for (String it: changes.keySet()) {
                List<Change> cs = sessionChanges.get(it);
                if (cs != null) {
                    changes.get(it).addAll(cs);
                }
            }

            cacheBack.merge(query, result, changes);
        }

        // compute the cache entries and put them in the cache.
        if (result != null && missKeys.size() > 0) {
            entries = cacheStore.computeCacheEntries(query, result);
            for (CacheEntry e: entries) {
                if (missKeys.contains(e.getKey())) {
                    boolean broadcast = isBroadcast(e.getKey());
                    if (broadcast) {
                        // broadcast mode, populate this key to all cache servers
                        int numServers = mc.getPool().getServers().length;
                        for (int i = 0; i < numServers; ++i) {
                            try {
                                mc.ciget(sessId, e.getKey(), e.asString(), i);
                                if (e.asString()) {
                                    mc.iqset(e.getKey(), e.getValue().toString(), i);
                                } else {
                                    byte[] bytes = cacheStore.serialize(e);
                                    mc.iqset(e.getKey(), bytes, i);
                                }
                                profile.incr(METRIC_IQSET);
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            } catch (IQException e1) {
                                e1.printStackTrace();
                            }
                        }
                    } else {
                        int hc = getHashCode(e.getKey());
                        try {
                            int size = e.getKey().length();
                            if (e.asString()) {
                                mc.iqset(e.getKey(), e.getValue().toString(), hc);
                                size += e.getValue().toString().length();
                            } else {
                                byte[] bytes = cacheStore.serialize(e);
                                mc.iqset(e.getKey(), bytes, hc);
                                size += bytes.length;
                            }
                            profile.incr(METRIC_IQSET);
                            if (stats != null) {
                                String type = e.getKey().split(",")[0];
                                stats.incrBy(type+"_size", size);
                                stats.incr(type);
                            }
                        } catch (IOException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        } catch (IQException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }
                }
            }
        }

        time = System.currentTimeMillis() - time;
        if (stats != null) {
            stats.incr(pre);
            stats.incrBy("latency_"+pre, time);
        }

        return result;
    }

    private boolean isBroadcast(String key) {
        int hc = cacheStore.getHashCode(key);
        return (hc == -1);
    }

    /**
     * Given a list of queries, return the results.
     * The size of the list returned is equal to the size of the list of queries provided as the input.
     * The result may be null if the query is invalid or no result is returned.
     * @param queries
     * @return
     * @throws Exception
     */
    public QueryResult[] readStatements(String... queries) throws Exception {
        if (stats != null)
            stats.incr(Stats.METRIC_READ_STATEMENTS);  

        Set<String> keys = new LinkedHashSet<>();
        Map<String, Set<String>> queries2Keys = new HashMap<>();
        for (String query: queries) {
            Set<String> s = cacheStore.getReferencedKeysFromQuery(query);            
            if (s.size() > 0) {
                keys.addAll(s);
                queries2Keys.put(query, s);
            }
        }

        TreeMap<Integer, List<String>> keysMap = new TreeMap<>();
        Set<CacheEntry> entries = new HashSet<>(); 
        boolean miss = false;

        if (keys == null || keys.size() == 0) {
            miss = true;
        } else {
            // build the hashCode map
            for (String key: keys) {
                Integer hc = getHashCode(key);
                if (hc == -1) {
                    Iterator<Integer> iter = hashToKeys.keySet().iterator();
                    if (iter.hasNext()) {
                        hc = iter.next();      // grab from any node.
                    }
                    
                    if (hc == -1) {
                        hc = 0;
                    }    
                }

                List<String> list = keysMap.get(hc);
                if (list == null) {
                    list = new ArrayList<>();
                    keysMap.put(hc, list);
                }
                if (!list.contains(key))
                    list.add(key);
            }

            // perform multiget on each set of keys in key map
            CacheEntry ce = null;
            for (Integer hc: keysMap.keySet()) {
                List<String> list = keysMap.get(hc);
                Collections.sort(list);
                String[] ks = list.toArray(new String[0]);
                Map<String, Object> res = mc.co_getMulti(sessId, hc, 0, false, ks);

                profile.incr(METRIC_COGETS_C);
                profile.incrBy(METRIC_KEYS_IN_COGETS_C, ks.length);

                for (String key: res.keySet()) {
                    Object val = res.get(key);
                    assert (val != null);
                    if (val instanceof String) {
                        ce = new CacheEntry(key, (String)val, true);
                    } else {
                        ce = cacheStore.deserialize(key, (byte[])val, buffer);
                    }
                    entries.add(ce);
                    list.remove(key);
                }

                if (list.size() > 0) {
                    miss = true;
                }
            }
        }

        // all cache hits
        if (!miss) {
            if (stats != null)
                stats.incrBy(Stats.METRIC_CACHE_HITS, keys.size());

            QueryResult results[] = new QueryResult[queries.length];
            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                results[i] = cacheStore.computeQueryResult(query, entries);
            }
            return results;
        }

        Set<String> missKeys = new HashSet<>();
        if (stats != null) {
            for (Integer hc: keysMap.keySet()) {
                missKeys.addAll(keysMap.get(hc));                    
            }

            if (missKeys.size() > 0) {
                if (mode == CachePolicy.WRITE_BACK) {
                    System.out.println("Missed keys "+name+" "+Arrays.toString(missKeys.toArray(new String[0])));
                }
                stats.incrBy(Stats.METRIC_CACHE_MISSES, missKeys.size()); 
            } else {
                stats.incr(Stats.METRIC_QUERY_NO_CACHE);
            }
        }

        // query the data store
        // only get the queries that relate to the missed keys to process
        for (String query: queries2Keys.keySet()) {
            boolean shouldQuery = false;
            Set<String> kss = queries2Keys.get(query);
            for (String k: kss) {
                if (missKeys.contains(k)) {
                    shouldQuery = true;
                    break;
                }
            }

            if (!shouldQuery) continue;

            QueryResult result = cacheStore.queryDataStore(query);
            profile.incr(METRIC_QUERIES);

            // at least one cache miss.
            if (mode == CachePolicy.WRITE_BACK) {
                Set<String> its = cacheBack.rationalizeRead(query);

                Map<Integer, List<String>> keyMap = new HashMap<>();
                Map<String, String> indexesKeyToBuffKey = new HashMap<>();

                // get the impacted sessions
                int hc = 0;
                for (String it: its) {
                    String buffKey = Util.getBufferedWriteKey(it);
                    String indexesKey = Util.getIndexesKey(buffKey);
                    hc = getHashCode(indexesKey);
                    List<String> list = keyMap.get(hc);
                    if (list == null) {
                        list = new ArrayList<>();
                        keyMap.put(hc, list);
                    }
                    list.add(indexesKey);
                    indexesKeyToBuffKey.put(indexesKey, buffKey);
                }

                List<String> sessIds = new ArrayList<>();

                // go to each server
                for (int hashCode: keyMap.keySet()) {
                    List<String> keyList = keyMap.get(hc);
                    Map<String, Object> res = mc.co_getMulti(sessId, hashCode, 1, true, keyList.toArray(new String[0]));
                    for (String key: keyList) {
                        Object bwIndexesVal = res.get(key);
                        int min = 0, max = 0;
                        if (bwIndexesVal != null) {                        
                            String[] minmax = ((String)bwIndexesVal).split(",");
                            min = Integer.parseInt(minmax[0]);
                            max = Integer.parseInt(minmax[1]);
                        }

                        String[] ks = new String[max-min+1+1];
                        String buffKey = indexesKeyToBuffKey.get(key);
                        for (int i = min; i <= max; ++i) {
                            ks[max-min] = Util.getBufferdWriteArchivePartitionKey(buffKey, i);
                        }
                        ks[ks.length-1] = indexesKeyToBuffKey.get(key);

                        Map<String, Object> res2 = mc.co_getMulti(sessId, hc, 1, true, ks);
                        for (int i = 0; i < ks.length; ++i) {
                            Object val = res2.get(ks[i]);
                            if (val != null) {
                                sessIds.addAll(MemcachedHelper.convertList((String)val));
                            }
                        }
                    }
                }

                // we are assuming there is just one hashCode here
                // TODO: must be fixed if the impacted buffered write key-value pairs are on multiple cache servers.
                String[] sessKeys = new String[sessIds.size()];
                for (int i = 0; i < sessIds.size(); ++i) {
                    sessKeys[i] = String.format("S-%s", sessIds.get(i));
                }

                Map<String, Object> res = mc.co_getMulti(sessId, hc, 1, false, sessKeys);
                List<Session> sessList = new ArrayList<>();
                for (int i = 0; i < sessKeys.length; ++i) {
                    Object val = res.get(sessKeys[i]);
                    if (val != null) {
                        Map<String, List<Change>> changesMap = cacheBack.deserializeSessionChanges((byte[])val);
                        if (changesMap != null && changesMap.size() > 0) {
                            Session sess = new Session(sessIds.get(i));
                            for (String it: changesMap.keySet()) {
                                List<Change> changes = changesMap.get(it);
                                for (Change c: changes) {
                                    sess.addChange(it, c);
                                }
                            }
                            sessList.add(sess);
                        }
                    }
                }

                LinkedHashMap<String, List<Change>> changes = new LinkedHashMap<>();
                for (Session s: sessList) {
                    for (String it: its) {
                        int idx = s.getIdentifiers().indexOf(it);
                        if (idx != -1) {
                            Change c = s.getChanges().get(idx);
                            List<Change> cs = changes.get(it);
                            if (cs == null) {
                                cs = new ArrayList<>();
                                changes.put(it, cs);
                            }
                            cs.add(c);
                        }
                    }
                }

                // combine with the local one
                for (String it: changes.keySet()) {
                    List<Change> cs = sessionChanges.get(it);
                    if (cs != null) {
                        changes.get(it).addAll(cs);
                    }
                }

                cacheBack.merge(query, result, changes);                
                //                Set<String> its = cacheBack.rationalizeRead(query);
                //                LinkedHashMap<String, List<Change>> buffVals = getBuffVals2(its, sessId, mc, hashCodes);
                //                
                //                // add with the local changes
                //                for (String it: buffVals.keySet()) {
                //                    List<Change> changes = sessionChanges.get(it);
                //                    if (changes != null) {
                //                        buffVals.get(it).addAll(changes);
                //                    }
                //                }
                //                result = cacheBack.merge(query, result, buffVals);
            }

            // compute the cache entries and put them in the cache.
            if (result != null && missKeys.size() > 0) {
                Set<CacheEntry> ces = cacheStore.computeCacheEntries(query, result);
                for (CacheEntry e: ces) {
                    if (missKeys.contains(e.getKey())) {        
                        try {
                            if (e.asString()) {
                                mc.iqset(e.getKey(), e.getValue().toString(), getHashCode(e.getKey()));
                            } else {
                                byte[] bytes = cacheStore.serialize(e);
                                mc.iqset(e.getKey(), bytes, getHashCode(e.getKey()));
                            }
                            profile.incr(METRIC_IQSET);
                        } catch (IOException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        } catch (IQException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }
                }
                entries.addAll(ces);
            }
        }

        QueryResult results[] = new QueryResult[queries.length];
        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];
            results[i] = cacheStore.computeQueryResult(query, entries);
        }
        return results;
    }

    List<Change> getChangesBytes(byte[] bytes, byte[] read_buffer) {
        List<Change> changes = new ArrayList<>();
        int offSet = 0;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (offSet < bytes.length) {
//            int len = buffer.getInt();
//            offSet += 4;

//            byte[] sidBytes = new byte[len];
//            buffer.get(sidBytes);
//            String sid = new String(sidBytes);
//            offSet += len;

//            int index = buffer.getInt();
//            offSet += 4;

            int len = buffer.getInt();
            offSet += 4;

            byte[] changeBytes = new byte[len];
            buffer.get(changeBytes);
            offSet += len;

            Change change = cacheBack.deserialize(changeBytes);
//            change.setSid(sid);
//            change.setSequenceId(index);

            changes.add(change);
        }
        return changes;
    }

    // This interval get hash code is only used for buffKey. 
    //    int getHashCodeBuffKey(String buffKey) {
    //        int hc = buffKey.hashCode() & 0xfffffff;
    //        return hc % mc.getPool().getServers().length;
    //    }

    int getHashCodeSessKey(MemcachedClient mc, String sessKey) {
        int hc = sessKey.hashCode() & 0xfffffff;
        return hc % mc.getPool().getServers().length;
    }

    //    static String getBufferedWritePartitionKey(String it) {
    //        int x = rand.nextInt(NUM_PARTITIONS_IT);
    //        return String.format("BW_%s_%s_%s", it, getStringIndex(x), BW_RESERVED_STR);
    //    }
    //    
    //    static String getBufferedWritePartitionKey(String it, int r) {
    //        return String.format("BW_%s_%s_%s", it, getStringIndex(r), BW_RESERVED_STR);
    //    }

    public static void main(String[] args) throws IOException {
        NgCache ngCache = new NgCache(null, null, null, CachePolicy.WRITE_BACK, 0, null, null, null, null, 
                false, 0, 1, 10);
        Dendrite d1 = ngCache.getDendrite("1");
        Dendrite d2 = ngCache.getDendrite("2");
        Dendrite d3 = ngCache.getDendrite("3");
        
        d1.insertInterval(10, 10, "k1");
        d1.insertInterval(11, 11, "k2");
        d2.insertInterval(10, 10, "k3");
        
        List<IntervalData> list = d1.overlappers(10, 13);
        for (IntervalData d: list) {
            System.out.println(String.format("Got interval min=%d max=%d keys=%s", d.getLower(), d.getUpper(), 
                    Arrays.toString(d.getKeys().toArray(new String[0]))));
        }
        
        list = d2.overlappers(10, 13);
        for (IntervalData d: list) {
            System.out.println(String.format("Got interval min=%d max=%d keys=%s", d.getLower(), d.getUpper(), 
                    Arrays.toString(d.getKeys().toArray(new String[0]))));
        }
    }

    /**
     * Performs a write statement.
     * @param dml
     * @return
     * @throws Exception 
     */
    //    public boolean writeStatement(String dml) throws Exception {
    //        if (stats != null)
    //            stats.incr(Stats.METRIC_WRITE_STATEMENTS);
    //        
    //        logger.debug("Write statement "+dml);
    //
    //        Set<String> keys = cacheStore.getImpactedKeysFromDml(dml);
    //        
    //        logger.debug("Impacted keys "+Arrays.toString(keys.toArray(new String[0])));
    //
    //        // write-around: delete impacted cache entries
    //        if (policy == CachePolicy.WRITE_AROUND) {
    //            for (String key: keys) {
    //                try {
    //                    int hc = getHashCode(key);
    //                    hashCodes.add(hc);
    //
    //                    mc.oqReg(sessId, key, hc);
    //                } catch (COException e) {
    //                    e.printStackTrace();
    //                }
    //            }
    //        }
    //
    //        // write-through or write-back: update cache entries
    //        if (policy == CachePolicy.WRITE_THROUGH || policy == CachePolicy.WRITE_BACK) {
    //            Map<String, Delta> deltas = cacheStore.updateCacheEntries(dml, keys);
    //            applyDeltas(sessId, deltas, hashCodes);
    //        }
    //
    //        // write-around or write-through: apply to the data store
    //        boolean success = false;
    //        if (policy == CachePolicy.WRITE_AROUND || policy == CachePolicy.WRITE_THROUGH) {
    //            success = cacheStore.dmlDataStore(dml);
    //        }
    //
    //        // write-back: buffer writes in the cache
    //        if (policy == CachePolicy.WRITE_BACK) {
    //            LinkedHashMap<String, Change> changes = cacheBack.rationalizeWrite(dml); 
    //            // reinforce the order
    //            List<String> its = new ArrayList<>();
    //            for (Map.Entry<String, Change> c : changes.entrySet()) {                
    //                its.add(c.getKey());
    //                c.getValue().setSid(sessId);
    //                c.getValue().setSequenceId(seqId);
    //                seqId++;
    //            }
    //  
    //            // append the data item identifiers in the session key-value pairs
    //            if (its != null && its.size() > 0) {
    //                String val = MemcachedHelper.convertString(its);            
    //                String sessKey = String.format("SESS-%s", sessId);
    //                success = mc.oqAppend(sessId, sessKey, getHashCodeSessKey(sessKey), val, true);
    //                if (!success) {
    //                    success = mc.oqAdd(sessId, sessKey, val, getHashCodeSessKey(sessKey), true);
    //                    if (success == false) {
    //                        logger.error("Something went wrong. Could not add a new key-value pair.");
    //                        System.exit(-1);
    //                    }
    //                    
    //                    if (stats != null)
    //                        stats.incr(Stats.METRIC_BUFFERED_SESSIONS);
    //                }
    //            }
    //
    //            // buffer the changes
    //            for (String it: changes.keySet()) {
    //                Change c = changes.get(it);                
    //                String buffKey = getBufferedWritePartitionKey(it);
    //                int hcBuffKey = getHashCodeBuffKey2(mc, buffKey);
    //                hashCodes.add(hcBuffKey);
    //
    //                byte[] bytes = cacheBack.serialize(c);       
    //                ByteBuffer bf = ByteBuffer.allocate((4+c.getSid().length())+4+(4+bytes.length));
    //                bf.putInt(c.getSid().length());
    //                bf.put(c.getSid().getBytes());
    //                bf.putInt(c.getSequenceId());
    //                bf.putInt(bytes.length);
    //                bf.put(bytes);
    //                success = mc.oqAppend(sessId, buffKey, hcBuffKey, bf.array(), false);
    //                
    //                if (success) {
    //                    logger.debug("Appended to buffKey "+buffKey+" change "+(String)c.getValue()+" successful.");
    //                }
    //
    //                boolean added = false;
    //                if (!success) {
    //                    added = mc.oqAdd(sessId, buffKey, bf.array(), hcBuffKey, false);
    //                    if (added) {
    //                        logger.debug("Added to buffKey "+buffKey+" change "+(String)c.getValue()+" successful.");
    //                    }
    //                    
    //                    if (added == false) {   // buff key exists but its value is full
    //                        // check the buffered write indexes
    //                        String bwIndexesKey = getBufferedWriteIndexesKey(it);
    //                        Object bwIndexesVal = mc.oqRead(sessId, bwIndexesKey, hcBuffKey, true);
    //                        int min = 0, max = 0;
    //                        if (bwIndexesVal != null) {                        
    //                            String[] minmax = ((String)bwIndexesVal).split(",");
    //                            min = Integer.parseInt(minmax[0]);
    //                            max = Integer.parseInt(minmax[1]);
    //                            max += 1;
    //                        }
    //                        success = mc.oqSwap(sessId, bwIndexesKey, hcBuffKey, String.format("%d,%d", min, max), true);
    //                        if (!success) {
    //                            logger.debug("Cannot swap the meta key "+bwIndexesKey);
    //                        }
    //                        
    //                        // swap the current value with the new changes.
    //                        Object obj = mc.oqRead(sessId, buffKey, hcBuffKey, false);
    //                        success = mc.oqWrite(sessId, buffKey, hcBuffKey, bf.array(), false);
    //                        if (!success) {
    //                            logger.debug("Cannot store the new change to buffKey "+buffKey);
    //                        }                        
    //                        
    //                        if (obj == null) {
    //                            logger.debug("Old value is null "+buffKey);
    //                        } else {                        
    //                            // create a new key-value pair for the old changes.
    //                            if (obj != null) {
    //                                String bwArchiveKey = getBufferdWriteArchivePartitionKey(buffKey, max);
    //                                success = mc.oqWrite(sessId, bwArchiveKey, hcBuffKey, obj, false);
    //                                if (!success) {
    //                                    logger.debug("Cannot store the archive key "+bwArchiveKey);
    //                                }
    //                            }
    //                        }
    //                    }
    //                }
    //
    //                // Append the buffKey to the teleW
    ////                if (added) {
    ////                    int buffKeyHc = buffKey.hashCode() & 0xfffffff;
    ////                    //int buffKeyHc = getHashCodeBuffKey(buffKey); 
    ////                    String teleW = BackgroundWorker.getEWLogKey(buffKeyHc % Config.NUM_PENDING_WRITES_LOGS);
    ////                    int teleHc = (buffKeyHc % Config.NUM_PENDING_WRITES_LOGS) % mc.getPool().getServers().length;
    ////                    hashCodes.add(teleHc);
    ////                    success = mc.oqAppend(sessId, teleW, teleHc, "-"+buffKey, true);
    ////                    if (!success) {
    ////                        added = mc.oqAdd(sessId, teleW, "-"+buffKey, teleHc, true);
    ////                    }
    ////                }
    //
    //                success = success || added;
    //            }
    //        }
    //
    //        return success;		
    //    }

    public boolean writeStatement(String dml) throws Exception {
        if (stats != null)
            stats.incr(Stats.METRIC_WRITE_STATEMENTS);

        logger.debug("Write statement "+dml);

        if (writer != null) 
            writer.println(dml);

        Set<String> keys = cacheStore.getImpactedKeysFromDml(dml);

        logger.debug("Impacted keys "+Arrays.toString(keys.toArray(new String[0])));

        // write-around: delete impacted cache entries
        if (mode == CachePolicy.WRITE_AROUND) {
            for (String key: keys) {
                try {
                    int hc = getHashCode(key);

                    mc.oqReg(sessId, key, hc);
                } catch (COException e) {
                    e.printStackTrace();
                }
            }
        }

        // write-through or write-back: generate deltas and store them locally
        if (mode == CachePolicy.WRITE_THROUGH || mode == CachePolicy.WRITE_BACK) {
            Map<String, Delta> deltas = cacheStore.updateCacheEntries(dml, keys);
            for (String key: deltas.keySet()) {
                List<Delta> ds = sessionDeltas.get(key);
                if (ds == null) {
                    ds = new ArrayList<>();
                    sessionDeltas.put(key,  ds);
                }
                ds.add(deltas.get(key));
            }
        }

        // write-around or write-through: update the data store
        boolean success = false;
        if (mode == CachePolicy.WRITE_AROUND || mode == CachePolicy.WRITE_THROUGH) {
            success = cacheStore.dmlDataStore(dml);
            profile.incr(METRIC_DMLS);
        } else {
            success = true;
        }

        // write-back: buffer writes in the cache
        if (mode == CachePolicy.WRITE_BACK) {
            LinkedHashMap<String, Change> changes = cacheBack.rationalizeWrite(dml);

            // reinforce the order
            List<String> its = new ArrayList<>();
            for (Map.Entry<String, Change> c : changes.entrySet()) {                
                its.add(c.getKey());
            }

            for (String it: changes.keySet()) {
                List<Change> cs = sessionChanges.get(it);
                if (cs == null) {
                    cs = new ArrayList<>();
                    sessionChanges.put(it, cs);
                }
                cs.add(changes.get(it));
            }
        }
        
        // for parts using RangeQC
        String name = cacheStore.getCollectionName(dml);
        
        if (name != null) {
            if (mode == CachePolicy.WRITE_THROUGH || mode == CachePolicy.WRITE_BACK) {
                Map<Interval1D, List<Delta>> deltas = cacheStore.updatePoints(dml);
                
                logger.debug("Check the deltas.");
                for (Interval1D interval: deltas.keySet()) {
                    List<Delta> ds = deltas.get(interval);
                    for (Delta d: ds) {
                        logger.debug("Delta type "+ d.getType() + " val "+d.getValue() + " for interval "+ interval.min + ","+interval.max);
                    }
                }
                
                Map<Interval1D, List<Delta>> pendingDeltas = pointDeltas.get(name);
                if (pendingDeltas == null) {
                    pointDeltas.put(name, deltas);
                } else {
                    for (Interval1D interval: deltas.keySet()) {
                        List<Delta> ds = deltas.get(interval);
                        List<Delta> ori_ds = pendingDeltas.get(interval);
                        if (ori_ds == null) {
                            pendingDeltas.put(interval, ds);
                        } else {
                            ori_ds.addAll(ds);
                        }
                    }
                }
            }
            
            if (mode == CachePolicy.WRITE_BACK) {
                Map<Interval1D, List<Change>> changes = cacheBack.bufferPoints(dml);
                Map<Interval1D, List<Change>> pendingChanges = pointChanges.get(name);
                if (pendingChanges == null) {
                    pointChanges.put(name, changes);
                } else {
                    for (Interval1D interval: changes.keySet()) {
                        List<Change> cs = changes.get(interval);
                        List<Change> ori_cs = pendingChanges.get(interval);
                        if (ori_cs == null) {
                            pendingChanges.put(interval, cs);
                        } else {
                            ori_cs.addAll(cs);
                        }
                    }
                }
            }
        }

        return success;     
    }    

    private void applyDeltas(String sid, Map<String, List<Delta>> deltasMap) throws COException, IOException {
        if (pointDeltas.size() > 0) {
            for (String name: pointDeltas.keySet()) {
                Dendrite dendrite = getDendrite(name);
                Map<Interval1D, List<Delta>> deltas = pointDeltas.get(name);
                
                for (Interval1D interval: deltas.keySet()) {
                    setPendingRangeLease(dendrite, interval, IntervalLock.WRITE);
                    List<IntervalData> intervals = dendrite.overlappers(interval.min, interval.max);
                    
                    for (IntervalData i: intervals) {
                        Set<String> keySet = i.getKeys();
                        
                        // read-modify-write to update the key
                        String key = keySet.iterator().next();
                        List<Delta> ds = deltasMap.get(key);
                        if (ds != null) {
                            ds.addAll(deltas.get(interval));
                        } else {
                            deltasMap.put(key, deltas.get(interval));
                        }
                    }
                    clearPendingRangeLease();
                }
            }
        }
        
        Map<Integer, List<String>> keysMap = new HashMap<>();
        Map<String, CacheEntry> res = new HashMap<>();

        for (String key: deltasMap.keySet()) {
            List<Delta> deltas = deltasMap.get(key);
            int hc = getHashCode(key);

            List<Delta> newDeltas = mergeDeltas(deltas);
            
            if (newDeltas.size() == 0) continue;
            
            if (newDeltas.size() == 1) {
                Delta delta = newDeltas.get(0);
                switch (delta.getType()) {
                case Delta.TYPE_APPEND:
                    byte[] bytes = cacheStore.serialize(delta);
                    mc.oqAppend(sid, key, hc, bytes, false);
                    profile.incr(METRIC_OQAPPEND);
                    break;
                case Delta.TYPE_INCR:
                    long val = (Integer)delta.getValue();
                    if (val > 0) {
                        mc.oqincr(sid, key, hc, val);
                        profile.incr(METRIC_OQINCR);
                    } else {                    
                        mc.oqdecr(sid, key, hc, val);
                        profile.incr(METRIC_OQDECR);
                    }
    
                    break;
                case Delta.TYPE_RMW:
                    // perform multi-reads
                    List<String> keys = keysMap.get(hc);
                    if (keys == null) {
                        keys = new ArrayList<>();
                        keysMap.put(hc, keys);
                    }
                    keys.add(key);
                    break;
                case Delta.TYPE_SET:
                    bytes = cacheStore.serialize(delta);
                    mc.oqSwap(sid, key, hc, bytes, false);
                    profile.incr(METRIC_OQWRITE);
                    break;
                }
            } else {
                // perform multi-reads
                List<String> keys = keysMap.get(hc);
                if (keys == null) {
                    keys = new ArrayList<>();
                    keysMap.put(hc, keys);
                }
                keys.add(key);
            }
        }

        for (Integer hc: keysMap.keySet()) {
            String[] keys = keysMap.get(hc).toArray(new String[0]);
            Map<String, Object> objs = mc.co_getMulti(sessId, hc, 1, false, keys);
            profile.incr(METRIC_COGETS_O);
            profile.incrBy(METRIC_KEYS_IN_COGETS_O, keys.length);

            List<String> keyList = new ArrayList<>();
            for (String key: objs.keySet()) {
                Object obj = objs.get(key);
                if (obj != null) {
                    CacheEntry ce = cacheStore.deserialize(key, obj, buffer);
                    res.put(key, ce);
                    keyList.add(key);
                }
            }

            Object[] values = new Object[keyList.size()];
            for (String key: res.keySet()) {
                CacheEntry ce = res.get(key);

                List<Delta> deltas = deltasMap.get(key);                    
                int i = keyList.indexOf(key);
                if (i < 0) continue;

                if (ce != null) {
                    for (Delta delta: deltas) {
                        ce = cacheStore.applyDelta(delta, ce);
                    }
                    
                    if (ce.asString()) {
                        values[i] = (String)ce.getValue();
                    } else {
                        byte[] bytes = cacheStore.serialize(ce);
                        values[i] = bytes;
                    }
                } else {
                    values[i] = null;
                }
            }

            if (keyList.size() > 0) {
                mc.oqSwaps(sessId, keyList.toArray(new String[0]), values, hc, false);
                profile.incr(METRIC_OQSWAPS);
                profile.incrBy(METRIC_KEYS_IN_OQSWAPS, keyList.size());
            }
        }
    }

    /**
     * Execute a query.
     * Start a session consisting of the query and cache operations to get the latest result.
     * @param query
     * @return
     * @throws Exception 
     */
    //    public Set<CacheEntry> read(String query) throws Exception {
    //        Set<String> keys = cacheStore.getReferencedKeysFromQuery(query);
    //        Set<CacheEntry> result = new HashSet<>();
    //
    //        boolean miss = false;
    //        Set<String> missKeys = new HashSet<>();
    //        for (String key: keys) {
    //            try {
    //                Object val = mc.iqget(key, getHashCode(key), false);
    //                if (val == null) {
    //                    miss = true;
    //                    missKeys.add(key);
    //                } else {
    //                    CacheEntry ce = cacheStore.deserialize(key, (byte[])val, buffer);
    //                    result.add(ce);
    //                }
    //            } catch (IOException e) {
    //                // TODO Auto-generated catch block
    //                e.printStackTrace();
    //            }
    //        }
    //
    //        if (!miss) {
    //            return result;
    //        }
    //
    //        // at least one cache miss.
    //        Set<CacheEntry> rs = processCacheMiss(missKeys, query);
    //        result.addAll(rs);
    //
    //        return result;
    //    }

    private List<Delta> mergeDeltas(List<Delta> deltas) {
        if (deltas.size() == 0) return null;
        
        List<Delta> newDeltas =new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        
        int type = Delta.TYPE_RMW;
        
        for (Delta d: deltas) {
            if (d.getType() == Delta.TYPE_APPEND || (d.getType() == Delta.TYPE_RMW && d.getValue() instanceof Integer)) {
                newDeltas.add(d);
                continue;
            }

            // only consider RMW and SET for now
            // TODO: handle INCR or APPEND
            if (d.getType() == Delta.TYPE_SET) {
                type = Delta.TYPE_SET;
                sb.append((String)d.getValue()+";");
            }    

            if (d.getType() == Delta.TYPE_RMW) {
                sb.append((String)d.getValue()+";");
            }
        }
        
        if (sb.length() > 0) {
            sb.setLength(sb.length()-1);
            newDeltas.add(new Delta(type, sb.toString()));
        }
        
        return newDeltas;
    }

    /**
     * Execute a dml.
     * Start a session consisting of the dml and cache operations to maintain the cache and the data store consistent.
     * @param dml
     * @return
     * @throws IOException 
     * @throws COException 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    //    public boolean write(String dml) {
    //        Set<String> keys = cacheStore.getImpactedKeysFromDml(dml);
    //
    //        // execute until the session succeeds
    //        Set<Integer> hashCodes = new HashSet<>();
    //        while (true) {
    //            String sid = mc.generateSID();
    //            hashCodes.clear();
    //
    //            try {
    //                // write-around: delete impacted cache entries
    //                if (policy == CachePolicy.WRITE_AROUND) {
    //                    for (String key: keys) {
    //                        try {
    //                            int hc = getHashCode(key);
    //                            hashCodes.add(hc);
    //
    //                            mc.oqReg(sid, key, hc);
    //                        } catch (COException e) {
    //                            // TODO Auto-generated catch block
    //                            e.printStackTrace();
    //                        }
    //                    }
    //                }
    //
    //                // write-through or write-back: update cache entries
    //                if (policy == CachePolicy.WRITE_THROUGH || policy == CachePolicy.WRITE_BACK) {
    //                    Map<String, Delta> deltas = cacheStore.updateCacheEntries(dml, keys);
    //                    applyDeltas(sid, deltas, hashCodes, false);
    //                }
    //
    //                // write-around or write-through: apply to the data store
    //                if (policy == CachePolicy.WRITE_AROUND || policy == CachePolicy.WRITE_THROUGH) {
    //                    cacheStore.dmlDataStore(dml);
    //                }
    //
    //                // write-back: buffer writes in the cache
    //                if (policy == CachePolicy.WRITE_BACK) {
    //                    Set<String> buffKeys = new HashSet<>();
    //                    keys.forEach(k -> buffKeys.addAll(cacheBack.getMapping(k)));
    //                    LinkedHashMap<String, Change> changes = cacheBack.bufferChanges(dml, buffKeys);
    //                    for (String buffKey: buffKeys) {
    //                        String teleW = Config.KEY_PRE_PENDING_WRITES_LOG + 
    //                                (getHashCodeBuffKey(buffKey) % Config.NUM_PENDING_WRITES_LOGS);
    //                        int teleHc = getHashCodeBuffKey(teleW);
    //                        boolean success = mc.oqAppend(sid, teleW, teleHc, ","+buffKey, true);
    //                    }
    //                    applyChanges(sid, changes, hashCodes, true);
    //                }
    //
    //                for (Integer hc: hashCodes) {
    //                    mc.validate(sid, hc);
    //                }
    //
    //                for (Integer hc: hashCodes) {
    //                    mc.dCommit(sid, hc);
    //                }
    //
    //                break;
    //            } catch (Exception e) {
    //                for (Integer hc: hashCodes) {
    //                    try {
    //                        mc.dAbort(sid, hc);
    //                    } catch (Exception e1) {
    //                        // TODO Auto-generated catch block
    //                        e1.printStackTrace();
    //                    }
    //                }
    //            }
    //        } // end while
    //
    //        return true;
    //    }

    private int bufferChanges(String sid, Map<String, List<Change>> changesMap) throws Exception {
        if (changesMap == null || changesMap.size() == 0) return 0;

        if (Config.logBuffWrites) {
            logBuffWrites(sid, changesMap);
        }

        int diff = 0;
        
        // create the session key-value pair and store it in the cache
        byte[] val = cacheBack.serializeSessionChanges(changesMap); 
        String sessKey = String.format("S-%s", sessId);
        int hc = getTagAsHashCode(sessKey);
        
        diff += (sessKey.length() + val.length) * Config.replicas;
        skv_diff += (sessKey.length() + val.length) * Config.replicas;

        Set<Integer> hcs = getReplicaHashCodes(hc);
        if (Config.sendParallel) {
            Future<Boolean>[] fs = new Future[Config.replicas];
            int idx = 0;
            for (final int i: hcs) {
                addedHashCodes.add(i);
                Future<Boolean> f = BackgroundService.getService().submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        return storeSessKeyValuePair(sessKey, i, val);
                    }
                });
                fs[idx++] = f;
            }

            Exception e = null;
            for (Future<Boolean> f: fs) {
                try {
                    f.get();
                } catch (Exception ex) {
                    e = ex;
                }
            }
            if (e != null) throw e;
        } else {
            for (final int i: hcs) {
                addedHashCodes.add(i);
                storeSessKeyValuePair(sessKey, i, val);
            }            
        }

        // append the session id to the data-item key-value pairs
        Map<Integer, List<String>> keyMap = new TreeMap<>();
        Map<Integer, List<String>> valueMap = new TreeMap<>();

        for (String it: changesMap.keySet()) {
            String buffKey = Util.getBufferedWriteKey(it);
            int hcBuffKey = getHashCode(buffKey);

            List<String> keys = keyMap.get(hcBuffKey);
            if (keys == null) {
                keys = new ArrayList<>();
                keyMap.put(hcBuffKey, keys);
            }
            keys.add(buffKey);

            List<String> values = valueMap.get(hcBuffKey);
            if (values == null) {
                values = new ArrayList<>();
                valueMap.put(hcBuffKey, values);                
            }
            
            String value = Config.DELIMITER+sessId;
            values.add(value);

            if (stats != null) {
                diff += value.length() * Config.replicas;
            }
        }
        
        // handle RangeQC
        if (pointChanges.size() > 0) {
            for (String name: pointChanges.keySet()) {
                Dendrite d = getDendrite(name);
                Map<Interval1D, List<Change>> changes = pointChanges.get(name);
                
                for (Interval1D interval: changes.keySet()) {                        
                    String key = String.format(Config.KEY_RANGE_ITEM_QUANTITY, name, interval.min, interval.max);
                    List<Change> cs = changes.get(interval);
                    if (cs.size() > 0) {
                        String str = "";
                        for (Change c: cs) {
                            str += Config.DELIMITER+sessId+","+c.getValue();
                        }
                        hc = getHashCode(key);
                        
                        int idx = keyMap.get(hc).indexOf(key);
                        if (idx == -1) {
                            keyMap.get(hc).add(key);
                            valueMap.get(hc).add(str);
                        } else {
                            String v = valueMap.get(hc).get(idx);
                            valueMap.get(hc).set(idx, v+str);
                        }
                    }
                    
                    setPendingRangeLease(d, interval, IntervalLock.WRITE);
                    d.insertBufferId(interval.min);
                    clearPendingRangeLease();
                }
            }
        }

        for (Integer hashCode : keyMap.keySet()) { 
            hcs = getReplicaHashCodes(hashCode);
            String[] keys = keyMap.get(hashCode).toArray(new String[0]);
            String[] values = valueMap.get(hashCode).toArray(new String[0]);
            
            if (Config.sendParallel) {
                Future<Boolean>[] fs = new Future[Config.replicas]; 
                int idx = 0;
                for (final int i : hcs) {
                    addedHashCodes.add(i);
                    Future<Boolean> f = BackgroundService.getService().submit(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            return appyBufferedWrite(sid, i, keys, values);
                        }
                    });
                    fs[idx++] = f;
                }

                Exception e = null;
                for (Future<Boolean> f: fs) {
                    try {
                        f.get();
                    } catch (Exception ex) {
                        e = ex;
                    }
                }
                if (e != null) throw e;
            } else {
                for (final int h: hcs) {
                    addedHashCodes.add(h);
                    appyBufferedWrite(sid, h, keys, values);
                }
            }
        }

        return diff;
    }

    Set<Integer> getReplicaHashCodes(Integer hashCode) {
        Set<Integer> hcs = new TreeSet<>();
        for (int i = 0; i < Config.replicas; ++i) {
            int x = (hashCode + i) % mc.getPool().getServers().length;
            hcs.add(x);
        }
        return hcs;
    }

    private int getTagAsHashCode(String sessKey) {
        if (tag != null) {
            try {
                int x = Integer.parseInt(tag);
                return x - 1;
            } catch (Exception e) {
                logger.fatal("Cannot get tag as a hash code.");
            }
        }
        
        return getHashCode(sessKey);
    }

    private int getHashCode(String buffKey) {
        Integer hc = keysToHash.get(buffKey);
        if (hc != null) return hc;
        
        hc = cacheStore.getHashCode(buffKey);
        if (hc == -1) {
            Iterator<Integer> iter = hashToKeys.keySet().iterator();
            if (iter.hasNext()) {
                hc = iter.next();
            }
            if (hc == -1) {
                hc = 0;
            }
        }
        
        keysToHash.put(buffKey, hc);
        
        Set<String> set = hashToKeys.get(hc);
        if (set == null) {
            set = new TreeSet<>();
            hashToKeys.put(hc, set);
        }
        set.add(buffKey);
        
        return hc;
    }

    private boolean storeSessKeyValuePair(String sessKey, int hc, byte[] val) throws IOException, COException {
        boolean success = mc.oqSwap(sessId, sessKey, hc, val, false);
        if (success) {
            if (stats != null && val != null) {
                diff += val.length + sessKey.length();
            }
        }
        return success;
    }

    protected Boolean appyBufferedWrite(String sid, int hc,
            String[] keys, String[] values) throws COException, IOException {
        byte[][] objVals = new byte[values.length][];
        for (int k = 0 ; k < values.length; ++k) objVals[k] = values[k].getBytes();

        Map<String, Boolean> res = mc.oqAppends(sid, keys, objVals, hc, false);
        profile.incr(METRIC_OQAPPENDS);
        profile.incrBy(METRIC_KEYS_IN_OQAPPENDS, keys.length);

        // If any append returns false, create a new key
        for (int k = 0; k < keys.length; ++k) {
            Boolean success = res.get(keys[k]);
            if (success != null && success.booleanValue() == false) {
                Util.appendOrAddSessId(keys[k], hc, sid, objVals[k], mc, false);
                if (stats != null) diff += keys[k].length();
            }
        }

        return null;
    }

    private void logBuffWrites(String sid, Map<String, List<Change>> changesMap) {
        if (bwWriter != null) {
            bwWriter.println(sid);
            for (String it: changesMap.keySet()) {
                bwWriter.print("It "+it+": ");
                for (Change c: changesMap.get(it)) {
                    bwWriter.println(c.toString());
                }
            }
        }
    }

    //    private void bufferChanges(String sid, Map<String, List<Change>> changesMap,
    //            Set<Integer> hashCodes) throws COException, IOException {
    //        if (changesMap.size() == 0) return;
    //        
    //        int i = 0;
    //        for (String it: changesMap.keySet()) {
    //            String buffKey = getBufferedWriteKey(it);
    //            int hcBuffKey = getHashCodeBuffKey2(mc, buffKey);
    //            hashCodes.add(hcBuffKey);
    //            
    //            List<Change> changes = changesMap.get(it);          
    //
    //            byte[] bytes = serializeChanges(changes);
    //
    //            boolean success = mc.oqAppend(sessId, buffKey, hcBuffKey, bytes, false); 
    //            profile.incr(METRIC_OQAPPEND);
    //            if (success) {
    //                logger.debug("Appended to buffKey "+buffKey+" successful.");
    //            }
    //
    //            if (!success) {
    //                boolean added = mc.oqAdd(sessId, buffKey, bytes, hcBuffKey, false);
    //                profile.incr(METRIC_OQADD);
    //                if (added) {
    //                    logger.debug("Added to buffKey "+buffKey+" successful.");
    //                }
    //                
    //                if (added == false) {   // buff key exists but its value is full
    //                    // check the buffered write indexes
    //                    String bwIndexesKey = getBufferedWriteIndexesKey(it);
    //                    Object bwIndexesVal = mc.oqRead(sessId, bwIndexesKey, hcBuffKey, true);
    //                    int min = 0, max = 0;
    //                    if (bwIndexesVal != null) {                        
    //                        String[] minmax = ((String)bwIndexesVal).split(",");
    //                        min = Integer.parseInt(minmax[0]);
    //                        max = Integer.parseInt(minmax[1]);
    //                        max += 1;
    //                    }
    //                    success = mc.oqSwap(sessId, bwIndexesKey, hcBuffKey, String.format("%d,%d", min, max), true);
    //                    if (!success) {
    //                        logger.debug("Cannot swap the meta key "+bwIndexesKey);
    //                    }
    //                    
    //                    // swap the current value with the new changes.
    //                    Object obj = mc.oqRead(sessId, buffKey, hcBuffKey, false);
    //                    success = mc.oqWrite(sessId, buffKey, hcBuffKey, bytes, false);
    //                    if (!success) {
    //                        logger.debug("Cannot store the new change to buffKey "+buffKey);
    //                    }                        
    //                    
    //                    if (obj == null) {
    //                        logger.debug("Old value is null "+buffKey);
    //                    } else {                        
    //                        // create a new key-value pair for the old changes.
    //                        if (obj != null) {
    //                            String bwArchiveKey = getBufferdWriteArchivePartitionKey(buffKey, max);
    //                            success = mc.oqWrite(sessId, bwArchiveKey, hcBuffKey, obj, false);
    //                            if (!success) {
    //                                logger.debug("Cannot store the archive key "+bwArchiveKey);
    //                            }
    //                        }
    //                    }
    //                }    
    //                
    //                if (added) {
    //                    int idx = rand.nextInt(Config.NUM_PENDING_WRITES_LOGS);
    //                    String ewKey = BackgroundWorker.getEWLogKey(idx);
    //                    success = mc.oqAppend(sessId, ewKey, idx, it+Config.DELIMITER, true);
    //                    profile.incr(METRIC_OQAPPEND);
    //                    if (!success) {
    //                        success = mc.oqAdd(sessId, ewKey, Config.DELIMITER+it+Config.DELIMITER, idx, true);
    //                        profile.incr(METRIC_OQADD);
    //                        if (!success) {
    //                            System.out.println("Cannot add it to TeleW.");
    //                        }
    //                    }
    //                }
    //            }
    //        }
    //        
    //        // append the data item identifiers in the session key-value pairs
    //        Set<String> its = changesMap.keySet();
    //        if (its != null && its.size() > 0) {
    //            String val = MemcachedHelper.convertString(its);            
    //            String sessKey = String.format("SESS-%s", sessId);
    //            boolean success = mc.oqAppend(sessId, sessKey, getHashCodeSessKey(mc, sessKey), val, true);
    //            if (!success) {
    //                success = mc.oqAdd(sessId, sessKey, val, getHashCodeSessKey(mc, sessKey), true);
    //                if (success == false) {
    //                    logger.error("Something went wrong. Could not add a new key-value pair.");
    //                    System.exit(-1);
    //                }
    //                
    //                if (stats != null)
    //                    stats.incr(Stats.METRIC_BUFFERED_SESSIONS);
    //            }
    //        }
    //    }

    byte[] serializeChanges(List<Change> changes) {
        ByteArrayOutputStream sb = new ByteArrayOutputStream();
        for (Change c: changes) {
            byte[] bytes = cacheBack.serialize(c);
            int size = 4+bytes.length;
            ByteBuffer bf = ByteBuffer.allocate(size);
//            bf.putInt(c.getSid().length());
//            bf.put(c.getSid().getBytes());
//            bf.putInt(c.getSequenceId());
            bf.putInt(bytes.length);
            bf.put(bytes); 
            sb.write(bf.array(), 0, size);
        }
        return sb.toByteArray();
    }

    //    private Set<CacheEntry> processCacheMiss(Set<String> missKeys, String query) throws Exception {
    //        // apply any pending buffered writes if write-back
    //        if (policy == CachePolicy.WRITE_BACK) {
    //            Set<String> buffKeys = new HashSet<>();
    //            missKeys.forEach(k -> buffKeys.addAll(cacheBack.getMapping(k)));
    //            applyBufferedWrites(mc, buffKeys, stats);
    //        }
    //
    //        for (String key: missKeys) {
    //            Object val = mc.iqget(key, getHashCode(key), false);
    //        }
    //
    //        // query the data store
    //        QueryResult result = cacheStore.queryDataStore(query);
    //
    //        // compute the cache entries and put them in the cache.
    //        Set<CacheEntry> entries = cacheStore.computeCacheEntries(query, result);
    //        for (CacheEntry e: entries) {
    //            if (missKeys.contains(e.getKey())) {
    //                byte[] bytes = cacheStore.serialize(e);
    //                try {
    //                    mc.iqset(e.getKey(), bytes, getHashCode(e.getKey()));
    //                } catch (IOException e1) {
    //                    // TODO Auto-generated catch block
    //                    e1.printStackTrace();
    //                } catch (IQException e1) {
    //                    // TODO Auto-generated catch block
    //                    e1.printStackTrace();
    //                }
    //            }
    //        }
    //
    //        return entries;
    //    }

    //    protected void applyBufferedWrites(MemcachedClient mc, Set<String> buffKeys, Stats stats) {
    //        Set<Integer> hcs = new HashSet<>();
    //        while (true) {
    //            String sid = mc.generateSID();
    //
    //            try {                
    //                int cnt = 0;
    //                for (String buffKey: buffKeys) {
    //                    int hc = getHashCodeBuffKey(buffKey);
    //                    hcs.add(hc);
    //                    Object obj = mc.oqRead(sid, buffKey, hc, false);
    //                    if (obj != null) {
    //                        byte[] bytes = (byte[]) obj;
    //                        CacheEntry entry = cacheStore.deserialize(buffKey, bytes, buffer);
    //                        cacheBack.applyBufferedWrite(entry.getKey(), entry.getValue());
    //                        mc.oqSwap(sid, buffKey, hc, null);
    //                        cnt++;
    //                    }
    //                }
    //
    //                for (Integer hc: hcs) {
    //                    mc.validate(sid, hc);
    //                }
    //
    //                for (Integer hc: hcs) {
    //                    mc.dCommit(sid, hc);
    //                }
    //
    //                if (stats != null)
    //                    stats.incrBy("applied_buff_writes", cnt);
    //
    //                break;
    //            } catch (Exception e) {
    //                for (Integer hc: hcs) {
    //                    try {
    //                        mc.dAbort(sid, hc);
    //                    } catch (Exception e1) {
    //                        // TODO Auto-generated catch block
    //                        e1.printStackTrace();
    //                    }
    //                }
    //            }
    //        }
    //    }

    public void clean() {
        if (bwWriter != null) {
            bwWriter.close();
        }
        if (writer != null) {
            writer.close();
        }

        BackgroundService.clean();
        
        try {
            dendriteSemaphore.acquire();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        for (Dendrite d: dendrites.values()) {
            d.shutdown();
        }
        dendrites.clear();
        dendriteSemaphore.release();
    }

    public void setBatch(int batch) {
        Config.BATCH = batch;
    }

    public void aggregateStats(Map<String, Map<String, Integer>> statsMap) {
        profile.aggregate(statsMap);
    }

    public void prettyPrint(Map<String, Map<String, Integer>> statsMap) {
        System.out.println("============ Session Profile =============");
        for (String name: statsMap.keySet()) {
            System.out.println("***** Session Type: "+name);
            Map<String, Integer> map = statsMap.get(name);
            if (map == null) System.out.println("No stats found.");
            else {
                int numSessions = 0;
                for (String metric: map.keySet()) {
                    switch (metric) {
                    case METRIC_SESSION_STARTS:
                        numSessions = map.get(metric);
                    case METRIC_SESSION_COMMITS:
                    case METRIC_SESSION_ABORTS:
                        System.out.println(String.format("%s: %d", metric, map.get(metric)));
                        break;
                    }
                }

                if (numSessions > 0) {
                    for (String metric: map.keySet()) {
                        if (metric.equals(METRIC_SESSION_STARTS) || metric.equals(METRIC_SESSION_STARTS) 
                                || metric.equals(METRIC_SESSION_STARTS) )
                            continue;

                        int val = map.get(metric);                        
                        double x = (val / (double)numSessions);
                        System.out.println(String.format("Metric %s: Value %.2f", metric, x));
                    }
                }
            }
        }
    }

    public String getSid() {
        return sessId;
    }
}