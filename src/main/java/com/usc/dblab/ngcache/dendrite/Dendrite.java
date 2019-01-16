package com.usc.dblab.ngcache.dendrite;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import edu.usc.dblab.itcomp.client.ITComp;
import edu.usc.dblab.itcomp.client.KeyHashCode;

public class Dendrite extends ITComp {

    public Dendrite(String domainName, int itcompindex, int domainSize, int numOfPartitions,
            int groupFactor, String[] itcomps, boolean stats, String poolName,
            String coord, KeyHashCode hc, boolean cacheTreeLogs,
            boolean cacheBufferLogs, boolean compactionThread,
            boolean bufferedReplication, ExecutorService parallelExecutor) {
        super(domainName, itcompindex, domainSize, numOfPartitions, groupFactor, itcomps, stats,
                poolName, coord, hc, cacheTreeLogs, cacheBufferLogs, compactionThread,
                bufferedReplication, parallelExecutor);
        // TODO Auto-generated constructor stub
    }

    public Map<Integer, List<BufferedWriteItem>> getBufferedWrite(int quantity,
            int quantity2) {
        // TODO Auto-generated method stub
        return null;
    }
}
