package com.epam.bigdata2016.model;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class VisitorsTokenResult implements Serializable {
    private long totalAmountVisitors;
    private Map<String,Long> tokenMap;

    public VisitorsTokenResult() {
        tokenMap = new HashMap<>();
        totalAmountVisitors =0 ;
    }

    public long getTotalAmountVisitors() {
        return totalAmountVisitors;
    }

    public void setTotalAmountVisitors(long totalAmountVisitors) {
        this.totalAmountVisitors = totalAmountVisitors;
    }

    public Map<String, Long> getTokenMap() {
        return tokenMap;
    }

    public void setTokenMap(Map<String, Long> tokenMap) {
        this.tokenMap = tokenMap;
    }

    @Override
    public String toString() {
        return "{" +
                "totalAmountVisitors=" + totalAmountVisitors +
                ", token_Map=" + tokenMap +
                '}';
    }
}

