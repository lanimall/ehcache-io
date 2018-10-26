package org.ehcache.extensions.io.impl.model;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Fabien Sanglier on 5/6/15.
 */

public class EhcacheStreamMaster implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private final ArrayList<ChunkDescriptor> chunkDescriptorList;
    private int writers = 0;
    private int readers = 0;
    private long lastReadTime = 0;
    private long lastWrittenTime = 0;

    public EhcacheStreamMaster() {
        this(0, 0);
    }

    private EhcacheStreamMaster(int writers, int readers) {
        this(writers, readers, 0L, 0L);
    }

    private EhcacheStreamMaster(int writers, int readers, long lastReadNanos, long lastWrittenTime) {
        this.chunkDescriptorList = new ArrayList<ChunkDescriptor>();
        this.writers = writers;
        this.readers = readers;
        this.lastReadTime = lastReadNanos;
        this.lastWrittenTime = lastWrittenTime;
    }

    public void resetChunkCount() {
        chunkDescriptorList.clear();
    }

    public void addChunk(int chunkIndex, long size, long checksum){
        chunkDescriptorList.add(new ChunkDescriptor(chunkIndex, size, checksum));
    }

    public int getChunkCount() {
        return chunkDescriptorList.size();
    }

    public int[] getAllChunkIndices() {
        int[] chunkIndexArray = new int[getChunkCount()];
        for(int i = 0 ; i < chunkDescriptorList.size(); i++){
            chunkIndexArray[i] = chunkDescriptorList.get(i).getChunkIndex();
        }
        return chunkIndexArray;
    }

    public long[] getAllChunkSizeInBytes() {
        long[] chunkSizeArray = new long[getChunkCount()];
        for(int i = 0 ; i < chunkDescriptorList.size(); i++){
            chunkSizeArray[i] = chunkDescriptorList.get(i).getSize();
        }
        return chunkSizeArray;
    }

    public long getChunksTotalSizeInBytes() {
        long totalSize = 0L;
        for(int i = 0 ; i < chunkDescriptorList.size(); i++){
            totalSize += chunkDescriptorList.get(i).getSize();
        }
        return totalSize;
    }

    public long[] getAllChunkChecksums() {
        long[] chunkChecksumArray = new long[getChunkCount()];
        for(int i = 0 ; i < chunkDescriptorList.size(); i++){
            chunkChecksumArray[i] = chunkDescriptorList.get(i).getChecksum();
        }
        return chunkChecksumArray;
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
        EhcacheStreamMaster newObj = new EhcacheStreamMaster(
                this.writers,
                this.readers,
                this.lastReadTime,
                this.lastWrittenTime);

        //adding the chunks
        for(ChunkDescriptor cd : this.chunkDescriptorList){
            newObj.addChunk(cd.chunkIndex, cd.size, cd.checksum);
        }

        return newObj;
    }

    private ArrayList<ChunkDescriptor> makeDeepCopyChunkDescriptor(ArrayList<ChunkDescriptor> old){
        ArrayList<ChunkDescriptor> copy = new ArrayList<ChunkDescriptor>(old.size());
        for(ChunkDescriptor chunkDescriptor : old){
            copy.add(chunkDescriptor.clone());
        }
        return copy;
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

        if (lastReadTime != that.lastReadTime) return false;
        if (lastWrittenTime != that.lastWrittenTime) return false;
        if (readers != that.readers) return false;
        if (writers != that.writers) return false;
        if (!chunkDescriptorList.equals(that.chunkDescriptorList)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = chunkDescriptorList.hashCode();
        result = 31 * result + writers;
        result = 31 * result + readers;
        result = 31 * result + (int) (lastReadTime ^ (lastReadTime >>> 32));
        result = 31 * result + (int) (lastWrittenTime ^ (lastWrittenTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "EhcacheStreamMaster{" +
                "chunkDescriptorList[size]=" + chunkDescriptorList.size() +
                ", writers=" + writers +
                ", readers=" + readers +
                ", lastReadTime=" + lastReadTime +
                ", lastWrittenTime=" + lastWrittenTime +
                '}' +
                ", hashcode=" + hashCode();
    }

    class ChunkDescriptor implements Serializable, Cloneable {
        private static final long serialVersionUID = 1L;

        private final int chunkIndex;
        private final long size;
        private final long checksum;

        ChunkDescriptor(int chunkIndex, long size, long checksum) {
            this.chunkIndex = chunkIndex;
            this.size = size;
            this.checksum = checksum;
        }

        public int getChunkIndex() {
            return chunkIndex;
        }

        public long getSize() {
            return size;
        }

        public long getChecksum() {
            return checksum;
        }

        @Override
        public ChunkDescriptor clone() {
            return new ChunkDescriptor(
                    this.chunkIndex,
                    this.size,
                    this.checksum);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ChunkDescriptor that = (ChunkDescriptor) o;

            if (checksum != that.checksum) return false;
            if (chunkIndex != that.chunkIndex) return false;
            if (size != that.size) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = chunkIndex;
            result = 31 * result + (int) (size ^ (size >>> 32));
            result = 31 * result + (int) (checksum ^ (checksum >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "ChunkDescriptor{" +
                    "chunkIndex=" + chunkIndex +
                    ", size=" + size +
                    ", checksum=" + checksum +
                    '}';
        }
    }

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
        WRITERS {
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
}