package com.usc.dblab.cafe;

public class Delta {
    public static final int TYPE_APPEND = 0;
    public static final int TYPE_RMW = 1;
    public static final int TYPE_INCR = 2;
    public static final int TYPE_SET = 3;
    
    // value of the delta
    final int type;
    final Object value;
    
    public Delta(int type, Object value) {
        this.type = type;
        this.value = value;
    }
    
    public int getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }
}
