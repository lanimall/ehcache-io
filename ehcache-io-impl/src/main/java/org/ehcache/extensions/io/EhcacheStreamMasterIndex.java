package org.ehcache.extensions.io;

import java.io.Serializable;

/**
 * Created by Fabien Sanglier on 5/6/15.
 */

/*package protected*/ class EhcacheStreamMasterIndex implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private int numberOfChunks;
    private StreamOpStatus status;

    EhcacheStreamMasterIndex(StreamOpStatus status) {
        this.numberOfChunks = 0;
        this.status = status;
    }

    EhcacheStreamMasterIndex(int numberOfChunk, StreamOpStatus status) {
        this.numberOfChunks = numberOfChunk;
        this.status = status;
    }

    public enum StreamOpStatus {
        CURRENT_WRITE, AVAILABLE
    }

    public int getAndIncrementChunkIndex(){
        return numberOfChunks++; //return the value before increment
    }

    public int getNumberOfChunk(){
        return numberOfChunks;
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
        return new EhcacheStreamMasterIndex(numberOfChunks, status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamMasterIndex that = (EhcacheStreamMasterIndex) o;

        if (numberOfChunks != that.numberOfChunks) return false;
        if (status != that.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = numberOfChunks;
        result = 31 * result + (status != null ? status.hashCode() : 0);
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