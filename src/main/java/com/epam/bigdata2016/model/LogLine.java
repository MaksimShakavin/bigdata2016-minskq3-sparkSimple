package com.epam.bigdata2016.model;

import java.io.Serializable;
import java.util.List;

public class LogLine implements Serializable{
    private Long cityId;
    private String timestamp;
    private String tagsId;

    public LogLine(Long cityId, String timestamp, String tagsId) {
        this.cityId = cityId;
        this.timestamp = timestamp;
        this.tagsId = tagsId;
    }

    public Long getCityId() {
        return cityId;
    }

    public void setCityId(Long cityId) {
        this.cityId = cityId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTagsId() {
        return tagsId;
    }

    public void setTagsId(String tagsId) {
        this.tagsId = tagsId;
    }
}
