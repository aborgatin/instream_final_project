package com.avborg.finalproject.straming.generator.model;

import java.net.Inet4Address;

public class Event {

    private String type;
    private String ip;
    private Long eventTime;
    private String url;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    public Event(String type, String ip, Long eventTime, String url) {
        this.type = type;
        this.ip = ip;
        this.eventTime = eventTime;
        this.url = url;
    }

    @Override
    public String toString() {
        return "Event{" +
                "type='" + type + '\'' +
                ", ip='" + ip + '\'' +
                ", eventTime=" + eventTime +
                ", url='" + url + '\'' +
                '}';
    }
}
