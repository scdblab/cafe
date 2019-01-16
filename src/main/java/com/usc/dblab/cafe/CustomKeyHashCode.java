package com.usc.dblab.cafe;

import java.util.Set;
import java.util.TreeSet;

import edu.usc.dblab.itcomp.client.KeyHashCode;

public class CustomKeyHashCode implements KeyHashCode {
    NgCache ngcache;
    Set<Integer> hashCodes = new TreeSet<>();
    
    public CustomKeyHashCode(NgCache ngcache) {
        this.ngcache = ngcache;
    }

    @Override
    public Integer getHashCode(String key) {
        int hc = ngcache.cacheStore.getHashCode(key);
        hashCodes.add(hc);
        return hc;
    }

    @Override
    public int[] getHashCodes() {
        int[] hcs = new int[hashCodes.size()];
        int idx = 0;
        for (int hc: hashCodes) {
            hcs[idx++] = hc;
        }
        return hcs;
    }

    @Override
    public int[] getReplicas(int id) {
        Set<Integer> set = ngcache.getReplicaHashCodes(id);
        hashCodes.addAll(set);
        int[] res = new int[set.size()];
        int cnt = 0;
        for (int hc: set) {
            res[cnt++] = hc;
        }
        return res;
    }

}
