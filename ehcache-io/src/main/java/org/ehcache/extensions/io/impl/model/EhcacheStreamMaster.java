package org.ehcache.extensions.io.impl.model;

import java.io.Serializable;

/**
 * Created by Fabien Sanglier on 5/6/15.
 */

public class EhcacheStreamMaster implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private int chunkCount = 0;
    private int writers = 0;
    private int readers = 0;
    private long lastReadTime = 0;
    private long lastWrittenTime = 0;

    public enum MutationType {
        INCREMENT, DECREMENT, MARK_NOW, INCREMENT_MARK_NOW, DECREMENT_MARK_NOW, NONE;
    }

    public enum ComparatorType {
        SINGLE_WRITER {
            @Override
            public boolean check(EhcacheStreamMaster streamMaster) {
                return null != streamMaster && streamMaster.getWriters() == 1;
            }
        },NO_READER_SINGLE_WRITER {
            @Override
            public boolean check(EhcacheStreamMaster streamMaster) {
                return null != streamMaster && streamMaster.getReaders() == 0 && streamMaster.getWriters() == 1;
            }
        },NO_WRITER {
            @Override
            public boolean check(EhcacheStreamMaster streamMaster) {
                return (null == streamMaster || null != streamMaster && streamMaster.getWriters() == 0);
            }
        },AT_LEAST_ONE_WRITER {
            @Override
            public boolean check(EhcacheStreamMaster streamMaster) {
                return null != streamMaster && streamMaster.getWriters() > 0;
            }
        },NO_READER {
            @Override
            public boolean check(EhcacheStreamMaster streamMaster) {
                return (null == streamMaster || null != streamMaster && streamMaster.getReaders() == 0);
            }
        },AT_LEAST_ONE_READER {
            @Override
            public boolean check(EhcacheStreamMaster streamMaster) {
                return null != streamMaster && streamMaster.getReaders() > 0;
            }
        },NO_READER_NO_WRITER {
            @Override
            public boolean check(EhcacheStreamMaster streamMaster) {
                return (null == streamMaster || null != streamMaster && streamMaster.getReaders() == 0 && streamMaster.getWriters() == 0);
            }
        },ANY {
            @Override
            public boolean check(EhcacheStreamMaster streamMaster) {
                return true;
            }
        };

        public abstract boolean check(EhcacheStreamMaster streamMaster);
    }

    public enum MutationField {
        CHUNKS {
            @Override
            public void mutate(EhcacheStreamMaster streamMaster, MutationType mutationType) {
                if(mutationType == MutationType.INCREMENT){
                    streamMaster.getAndIncrementChunkCount();
                } else if (mutationType == MutationType.MARK_NOW){
                    streamMaster.setWrittenNow();
                } else if (mutationType == MutationType.INCREMENT_MARK_NOW){
                    streamMaster.getAndIncrementChunkCount();
                    streamMaster.setWrittenNow();
                } else if (mutationType == MutationType.DECREMENT_MARK_NOW){
                    throw new IllegalStateException("Not supported");
                } else if (mutationType == MutationType.DECREMENT){
                    throw new IllegalStateException("Not supported");
                } else if (mutationType == MutationType.NONE){
                    ;;
                } else {
                    throw new IllegalStateException("Not supported");
                }
            }
        }, WRITERS {
            @Override
            public void mutate(EhcacheStreamMaster streamMaster, MutationType mutationType) {
                if(mutationType == MutationType.INCREMENT){
                    streamMaster.addWriter();
                } else if (mutationType == MutationType.MARK_NOW){
                    streamMaster.setWrittenNow();
                } else if (mutationType == MutationType.INCREMENT_MARK_NOW){
                    streamMaster.addWriter();
                    streamMaster.setWrittenNow();
                } else if (mutationType == MutationType.DECREMENT){
                    streamMaster.removeWriter();
                } else if (mutationType == MutationType.DECREMENT_MARK_NOW){
                    streamMaster.removeWriter();
                    streamMaster.setWrittenNow();
                } else if (mutationType == MutationType.NONE){
                    ;;
                } else {
                    throw new IllegalStateException("Not supported");
                }
            }
        }, READERS {
            @Override
            public void mutate(EhcacheStreamMaster streamMaster, MutationType mutationType) {
                if(mutationType == MutationType.INCREMENT){
                    streamMaster.addReader();
                } else if (mutationType == MutationType.MARK_NOW){
                    streamMaster.setReadNow();
                } else if (mutationType == MutationType.INCREMENT_MARK_NOW){
                    streamMaster.addReader();
                    streamMaster.setReadNow();
                } else if (mutationType == MutationType.DECREMENT){
                    streamMaster.removeReader();
                } else if (mutationType == MutationType.DECREMENT_MARK_NOW){
                    streamMaster.removeReader();
                    streamMaster.setReadNow();
                } else if (mutationType == MutationType.NONE){
                    ;;
                } else {
                    throw new IllegalStateException("Not supported");
                }
            }
        };

        public abstract void mutate(EhcacheStreamMaster streamMaster, MutationType mutationType);
    }

    public EhcacheStreamMaster() {
        this(0);
    }

    public EhcacheStreamMaster(int chunkCount) {
        this(chunkCount, 0, 0);
    }

    public EhcacheStreamMaster(int chunkCount, int writers, int readers) {
        this.chunkCount = chunkCount;
        this.writers = writers;
        this.readers = readers;
    }

    private EhcacheStreamMaster(int chunkCount, int writers, int readers, long lastReadNanos, long lastWrittenTime) {
        this.chunkCount = chunkCount;
        this.writers = writers;
        this.readers = readers;
        this.lastReadTime = lastReadNanos;
        this.lastWrittenTime = lastWrittenTime;
    }

    public int getAndIncrementChunkCount() {
        return chunkCount++;
    }

    public void resetChunkCount() {
        chunkCount = 0;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public void addWriter() {
        writers++;
    }

    public void removeWriter() {
        writers--;
    }

    public int getWriters() {
        return writers;
    }

    public void addReader() {
        readers++;
    }

    public void removeReader() {
        readers--;
    }

    public int getReaders() {
        return readers;
    }

    public long getLastWrittenTime() {
        return lastWrittenTime;
    }

    private void setWrittenNow(){
        lastWrittenTime = System.currentTimeMillis();
    }

    public long getLastReadTime() {
        return lastReadTime;
    }

    private void setReadNow(){
        lastReadTime = System.currentTimeMillis();
    }

    @Override
    public EhcacheStreamMaster clone() {
        return new EhcacheStreamMaster(
                this.chunkCount,
                this.writers,
                this.readers,
                this.lastReadTime,
                this.lastWrittenTime);
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

    public boolean equalsNoReadWriteTimes(Object o) {
        return equals(o, true, true);
    }

    public boolean equals(Object o, boolean noCompareReadTime, boolean noCompareWriteTime) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamMaster that = (EhcacheStreamMaster) o;

        if (chunkCount != that.chunkCount) return false;
        if (readers != that.readers) return false;
        if (writers != that.writers) return false;
        if (!noCompareReadTime && lastReadTime != that.lastReadTime) return false;
        if (!noCompareWriteTime && lastWrittenTime != that.lastWrittenTime) return false;

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamMaster that = (EhcacheStreamMaster) o;

        if (chunkCount != that.chunkCount) return false;
        if (lastReadTime != that.lastReadTime) return false;
        if (lastWrittenTime != that.lastWrittenTime) return false;
        if (readers != that.readers) return false;
        if (writers != that.writers) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = chunkCount;
        result = 31 * result + writers;
        result = 31 * result + readers;
        result = 31 * result + (int) (lastReadTime ^ (lastReadTime >>> 32));
        result = 31 * result + (int) (lastWrittenTime ^ (lastWrittenTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "EhcacheStreamMaster{" +
                "chunkCount=" + chunkCount +
                ", writers=" + writers +
                ", readers=" + readers +
                ", lastReadTime=" + lastReadTime +
                ", lastWrittenTime=" + lastWrittenTime +
                '}' +
                ", hashcode=" + hashCode();
    }
}