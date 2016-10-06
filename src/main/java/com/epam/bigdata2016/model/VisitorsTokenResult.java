package com.epam.bigdata2016.model;


import java.util.HashMap;
import java.util.Map;

public class VisitorsTokenResult {
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
}

