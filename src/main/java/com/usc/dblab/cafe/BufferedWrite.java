//package com.usc.dblab.cafe;
//
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.TreeMap;
//
///**
// * Represents a buffered write stored locally.
// * @author hieun
// *
// */
//public class BufferedWrite {
//    String it;
//    
//    int minIdx;
//    int maxIdx;
//    
//    TreeMap<Integer, List<Change>> archivedChanges;
//    List<Change> currChanges;
//    
//    public BufferedWrite(String it, int minIdx, int maxIdx, TreeMap<Integer, 
//            List<Change>> archivedChanges, List<Change> currChanges) {
//        this.it = it;
//        this.minIdx = minIdx;
//        this.maxIdx = maxIdx;
//        this.archivedChanges = archivedChanges;
//        this.currChanges = currChanges;
//    }
//
//    public List<String> getFirstKSessions(int batch) {
//        List<String> sessIds = new ArrayList<>();
//        Set<String> set = new HashSet<>();
//        
//        int curr = 0;
//        for (Integer x: archivedChanges.keySet()) {
//            if (curr > batch) break;
//            
//            List<Change> changes = archivedChanges.get(x);
//            if (changes != null) {
//                for (Change c: changes) {
//                    if (curr > batch) break;
//                    
//                    if (!set.contains(c.getSid())) {
//                        sessIds.add(c.getSid());
//                        curr++;
//                    }
//                }
//            }
//        }
//        
//        if (currChanges != null) {
//            for (Change c: currChanges) {
//                if (curr > batch) break;
//                
//                if (!set.contains(c.getSid())) {
//                    sessIds.add(c.getSid());
//                    curr++;
//                }
//            }
//        }
//        
//        return sessIds;
//    }
//
//    public List<String> getDependedSessions(String sid) {
//        List<String> sessIds = new ArrayList<>();
//        Set<String> set = new HashSet<>();
//        
//        boolean stop = false;
//        for (Integer x: archivedChanges.keySet()) {
//            List<Change> changes = archivedChanges.get(x);
//            if (changes != null) {
//                for (Change c: changes) {
//                    if (c.getSid().equals(sid)) {
//                        stop = true;
//                        break;
//                    }
//                    
//                    if (!set.contains(c.getSid())) {
//                        sessIds.add(c.getSid());
//                    }
//                }
//            }
//        }
//        
//        if (!stop) {
//            if (currChanges != null) {
//                for (Change c: currChanges) {
//                    if (c.getSid().equals(sid)) {
//                        if (!set.contains(sid))
//                            sessIds.add(sid);
//                        stop = true;
//                        break;
//                    }
//                    
//                    if (!set.contains(c.getSid())) {
//                        sessIds.add(c.getSid());
//                    }
//                }
//            }
//        }
//        
//        return sessIds;
//    }
//
//    public List<Change> getRelatedChanges(Set<String> visitedSessions) {
//        List<Change> changes = new ArrayList<>();
//        
//        for (Integer x: archivedChanges.keySet()) {
//            List<Change> cs = archivedChanges.get(x);
//            
//            for (Change c: cs) {
//                if (!visitedSessions.contains(c.getSid())) {
//                    break;
//                } else {
//                    changes.add(c);
//                }
//            }
//        }
//        
//        for (Change c: currChanges) {
//            if (!visitedSessions.contains(c.getSid())) {
//                break;
//            } else {
//                changes.add(c);
//            }
//        }
//        
//        return changes;
//    }
//
//    /**
//     * Merge all changes from different key-value pairs into one list.
//     * @return
//     */
//    public List<Change> getChanges() {
//        List<Change> changes = new ArrayList<>();
//        for (Integer x: archivedChanges.keySet()) {
//            changes.addAll(archivedChanges.get(x));
//        }
//        
//        if (currChanges != null) {
//            changes.addAll(currChanges);
//        }
//        
//        return changes;
//    }
//
//    public Map<Integer, List<Change>> getUpdatedKeyValues(List<String> sessIds) {
//        Map<Integer, List<Change>> res = new LinkedHashMap<>();
//        
//        for (Integer x: archivedChanges.keySet()) {
//            List<Change> changes = archivedChanges.get(x);
//            int i = 0;
//            
//            boolean modified = false;
//            while (i < changes.size()) {
//                if (sessIds.contains(changes.get(i).getSid())) {
//                    changes.remove(i);
//                    modified = true;
//                } else {
//                    i++;
//                }
//            }
//            
//            if (modified) {
//                res.put(x, changes);
//            }
//        }
//        
//        if (currChanges != null) {
//            int i = 0;
//            boolean modified = false;
//            while (i < currChanges.size()) {
//                if (sessIds.contains(currChanges.get(i).getSid())) {
//                    currChanges.remove(i);
//                    modified = true;
//                } else {
//                    i++;
//                }
//            }
//            
//            if (modified) {
//                res.put(-1, currChanges);
//            }
//        }
//        
////        if (res.size() > 0) {
////            System.out.println("Modified "+it);
////        }
//        
//        return res;
//    }
//}
