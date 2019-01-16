package com.usc.dblab.cafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a session.
 * @author Hieu
 *
 */
public class Session {
    private final String sid;

    private final List<String> its;
    private final List<Change> changes;

    private final Set<Session> dependants;	// list of sessions that depends on this session
    private final Set<Session> depends;

    public Session(String sid) {
        this.sid = sid;
        this.its = new ArrayList<>();
        this.changes = new ArrayList<>();

        this.dependants = new HashSet<>();
        this.depends = new HashSet<>();
    }

    public void addChange(String it, Change change) {
        this.its.add(it);
        this.changes.add(change);
    }

    public List<String> getIdentifiers() {
        return this.its;
    }
    
    public Change getChange(int idx) {
        if (idx < 0 || idx >= changes.size()) return null;
        return changes.get(idx);
    }
    
    public List<Change> getChanges() {
        return changes;
    }

    public String getSid() {
        return sid;
    }

    public Set<Session> getDependants() {
        return dependants;
    }

    public Set<Session> getDepends() {
        return depends;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s\n", sid));
        for (int i = 0; i < its.size(); ++i) {
            sb.append(its.get(i)+ " ");
            sb.append(changes.get(i).toString());
            sb.append("\n");
        }

        sb.append(String.format("dep:%d", depends.size()));
        sb.append(",dependants:");
        for (Session s: dependants) {
            sb.append(s.getSid()+",");
        }
        sb.append("depends:");
        for (Session s: depends) {
            sb.append(s.getSid()+",");
        }
        return sb.toString();
    }

//    public void sortChanges() {
//        List<ChangeWrapper> wrappers = new ArrayList<>();
//        for (int i = 0; i < its.size(); ++i) {
//            wrappers.add(new ChangeWrapper(its.get(i), changes.get(i)));
//        }
//        Collections.sort(wrappers);
//        
//        its.clear();
//        changes.clear();        
//        for (ChangeWrapper w: wrappers) {
//            its.add(w.it);
//            changes.add(w.change);
//        }
//    }
}

//class ChangeWrapper implements Comparable<ChangeWrapper> {
//    String it;
//    Change change;
//    
//    public ChangeWrapper(String it, Change change) {
//        this.it = it;
//        this.change = change;
//    }
//
//    @Override
//    public int compareTo(ChangeWrapper o) {
//        return change.getSequenceId() - o.change.getSequenceId();
//    }
//}