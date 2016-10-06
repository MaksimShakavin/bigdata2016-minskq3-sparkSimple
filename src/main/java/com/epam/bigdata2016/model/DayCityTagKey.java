package com.epam.bigdata2016.model;

public class DayCityTagKey {
    private String day;
    private String city;
    private String tag;

    public DayCityTagKey() {
    }

    public DayCityTagKey(String day, String city, String tag) {
        this.day = day;
        this.city = city;
        this.tag = tag;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DayCityTagKey that = (DayCityTagKey) o;

        if (!day.equals(that.day)) return false;
        if (!city.equals(that.city)) return false;
        return tag.equals(that.tag);

    }

    @Override
    public int hashCode() {
        int result = day.hashCode();
        result = 31 * result + city.hashCode();
        result = 31 * result + tag.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Key{" +
                "day='" + day + '\'' +
                ", city='" + city + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }
}
