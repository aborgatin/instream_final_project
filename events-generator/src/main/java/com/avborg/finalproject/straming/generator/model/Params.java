package com.avborg.finalproject.straming.generator.model;

public class Params {
    private long count;
    private boolean isBot;
    private long delay;
    private long iter;
    private String path;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public boolean isBot() {
        return isBot;
    }

    public void setBot(boolean bot) {
        isBot = bot;
    }

    public long getDelay() {
        return delay;
    }

    public long getIter() {
        return iter;
    }

    public void setIter(long iter) {
        this.iter = iter;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
