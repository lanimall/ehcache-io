package org.ehcache.extensions.io.impl;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Fabien Sanglier on 5/6/15.
 */

public class EhcacheStreamMaster implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AtomicInteger chunkCounter;
    private StreamOpStatus status = StreamOpStatus.AVAILABLE;

    public EhcacheStreamMaster(StreamOpStatus status) {
        this.chunkCounter = new AtomicInteger(0);
        this.status = status;
    }

    public EhcacheStreamMaster(int numberOfChunk, StreamOpStatus status) {
        this.chunkCounter = new AtomicInteger(numberOfChunk);
        this.status = status;
    }

    public EhcacheStreamMaster(EhcacheStreamMaster master) {
        this.chunkCounter = new AtomicInteger(master.chunkCounter.intValue());
        this.status = master.status;
    }

    public enum StreamOpStatus {
        CURRENT_WRITE, AVAILABLE
    }

    public int getChunkCounter() {
        return chunkCounter.get();
    }

    public int getAndIncrementChunkCounter() {
        return chunkCounter.getAndIncrement();
    }

    public boolean isCurrentWrite(){
        return status == StreamOpStatus.CURRENT_WRITE;
    }

    public synchronized void setCurrentWrite() {
        this.status = StreamOpStatus.CURRENT_WRITE;
    }

    public synchronized void setAvailable() {
        this.status = StreamOpStatus.AVAILABLE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamMaster that = (EhcacheStreamMaster) o;

        if (getChunkCounter() != that.getChunkCounter()) return false;
        if (status != that.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getChunkCounter();
        result = 31 * result + status.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EhcacheStreamMaster{" +
                "numberOfChunks=" + getChunkCounter() +
                ", status=" + status +
                '}';
    }
}