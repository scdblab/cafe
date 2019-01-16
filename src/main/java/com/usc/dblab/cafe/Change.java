package com.usc.dblab.cafe;

/**
 * Represents a buffered write change.
 * Each change is a delta with additional metadata: sid, sequence
 * @author hieun
 *
 */
public class Change extends Delta {
//    private String sid;
//    private int sequenceId;

    public Change(int type, Object value) {
        super(type, value);
    }

//    public void setSequenceId(int i) {
//        sequenceId = i;
//    }
//
//    public String getSid() {
//        return sid;
//    }

//    public void setSid(String sessId) {
//        this.sid = sessId;
//    }
//
//    public int getSequenceId() {
//        return sequenceId;
//    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Change");
//        sb.append(" sid="+sid);
//        sb.append(" sequenceId="+sequenceId);
        sb.append(" type="+type);
        sb.append(" val="+value.toString());
        return sb.toString();
    }
}
