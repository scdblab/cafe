package com.usc.dblab.cafe;

public class Config {
	public static final int NUM_PENDING_WRITES_LOGS = 1;
	
	public static final String KEY_PRE_PENDING_WRITES_LOG = "TW";
    public static final String KEY_RANGE_ITEM_QUANTITY = "rbw_dm_p,%s,%d,%d,xxxx";
    
	public static final String DELIMITER = ";";
	public static final int ALPHA = 1;
	public static final int LEASE_BACKOFF_TIME = 10;
	public static int BATCH = 50;
    public static int AR_SLEEP = 0;
    public static final boolean logBuffWrites = false;
    public static boolean storeCommitedSessions = false;
    public static int replicas = 1;
    public static boolean sendParallel = false;
    
    public static final int BDB_ASYNC = 1;
    public static final int BDB_SYNC = 2;
    public static final int NO_PERSIST = 3;

    public static final int BW_CHECK_POINT = 100000;
    public static int persistMode = NO_PERSIST;
}
