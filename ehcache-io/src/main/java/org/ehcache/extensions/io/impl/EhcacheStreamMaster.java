package org.ehcache.extensions.io.impl;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Fabien Sanglier on 5/6/15.
 */

/*package protected*/ class EhcacheStreamMaster implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private final AtomicInteger numberOfChunks;
    private StreamOpStatus status = StreamOpStatus.AVAILABLE;

    public EhcacheStreamMaster(StreamOpStatus status) {
        this.numberOfChunks = new AtomicInteger(0);
        this.status = status;
    }

    public EhcacheStreamMaster(int numberOfChunk, StreamOpStatus status) {
        this.numberOfChunks = new AtomicInteger(numberOfChunk);
        this.status = status;
    }

    public enum StreamOpStatus {
        CURRENT_WRITE, AVAILABLE
    }

    public int getAndIncrementChunkIndex(){
        return numberOfChunks.getAndIncrement(); //return the value before increment
    }

    public int getNumberOfChunk(){
        return numberOfChunks.get();
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
    public Object clone() {
        return new EhcacheStreamMaster(numberOfChunks.get(), status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamMaster that = (EhcacheStreamMaster) o;

        if (!numberOfChunks.equals(that.numberOfChunks)) return false;
        if (status != that.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = numberOfChunks.hashCode();
        result = 31 * result + status.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EhcacheStreamMasterIndex{" +
                "numberOfChunks=" + numberOfChunks +
                ", status=" + status +
                '}';
    }
}