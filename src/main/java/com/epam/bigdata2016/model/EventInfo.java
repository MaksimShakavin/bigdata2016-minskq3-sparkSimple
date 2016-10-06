package com.epam.bigdata2016.model;


import java.io.Serializable;

public class EventInfo implements Serializable {
    private String id;
    private String name;
    private String description;
    private int attendance;

    public EventInfo(String id, String name, String description,  int attendance) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.attendance = attendance;
    }

    //GETTERS AND SETTERS

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getAttendance() {
        return attendance;
    }

    public void setAttendance(int attendance) {
        this.attendance = attendance;
    }

}
