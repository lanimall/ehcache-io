package org.ehcache.extensions.io.impl.model;

import java.io.Serializable;

/**
 * Created by Fabien Sanglier on 5/6/15.
 */

//immutable!!
public class EhcacheStreamMaster implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private final int chunkCount;
    private final StreamOpStatus state;
    private final long createdNanos; //to capture successive changes and use to validate equals/not equals

    public enum StreamOpStatus {
        CURRENT_WRITE, AVAILABLE
    }

    public EhcacheStreamMaster(final StreamOpStatus status) {
        this(0, status, System.nanoTime());
    }

    public EhcacheStreamMaster(final int chunkCount, final StreamOpStatus status) {
        this(chunkCount, status, System.nanoTime());
    }

    private EhcacheStreamMaster(final int chunkCount, final StreamOpStatus status, final long createdNanos) {
        this.chunkCount = chunkCount;
        this.state = (null == status)?status.AVAILABLE:status; //make sure enum is never stored as null
        this.createdNanos = createdNanos;
    }

    @Override
    protected EhcacheStreamMaster clone() {
        return new EhcacheStreamMaster(
                this.chunkCount,
                this.state,
                this.createdNanos
        );
    }

    public EhcacheStreamMaster newWithIncrementCount(){
        return new EhcacheStreamMaster(
                this.chunkCount+1,
                this.state
        );
    }

    public EhcacheStreamMaster newWithStateChange(final StreamOpStatus status){
        return new EhcacheStreamMaster(
                this.chunkCount,
                status
        );
    }

    public static EhcacheStreamMaster deepCopy(final EhcacheStreamMaster obj){
        return (null != obj)?obj.clone():null;
    }

    public static boolean compare(EhcacheStreamMaster thisObject, EhcacheStreamMaster thatObject){
        if(thisObject == null && thatObject == null)
            return true;

        if(thisObject != null && thatObject == null || thisObject == null && thatObject != null)
            return false;

        return thisObject.equals(thatObject);
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public boolean isCurrentWrite(){
        return state == StreamOpStatus.CURRENT_WRITE;
    }

//    public int getAndIncrementChunkCounter() {
//        return chunkCounter.getAndIncrement();
//    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//
//        EhcacheStreamMaster that = (EhcacheStreamMaster) o;
//
//        if (chunkCounter.get() != that.chunkCounter.get()) return false;
//        if (state != that.state) return false;
//        if (createdNanos != that.createdNanos) return false;
//
//        return true;
//    }
//
//    @Override
//    public int hashCode() {
//        int result = chunkCounter.hashCode();
//        result = 31 * result + (int) (createdNanos ^ (createdNanos >>> 32));
//        result = 31 * result + status.hashCode();
//        return result;
//    }
//
//    @Override
//    public String toString() {
//        return "EhcacheStreamMaster{" +
//                "chunkCounter=" + chunkCounter +
//                ", createdNanos=" + createdNanos +
//                ", status=" + status +
//                '}';
//    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamMaster that = (EhcacheStreamMaster) o;

        if (chunkCount != that.chunkCount) return false;
        if (createdNanos != that.createdNanos) return false;
        if (state != that.state) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = chunkCount;
        result = 31 * result + (int) (createdNanos ^ (createdNanos >>> 32));
        result = 31 * result + state.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EhcacheStreamMaster{" +
                "chunkCount=" + chunkCount +
                ", createdNanos=" + createdNanos +
                ", state=" + state +
                '}';
    }
}