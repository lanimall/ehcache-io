package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 * Created by FabienSanglier on 5/6/15.
 */

/*package protected*/ abstract class BaseEhcacheStream {
    public static final long LOCK_TIMEOUT = 10000;

    /*
     * The Internal Ehcache cache object
     */
    final Cache cache;

    /*
     * The Ehcache cache key object the data should get written to
     */
    final Object cacheKey;

    BaseEhcacheStream(Cache cache, Object cacheKey) {
        this.cache = cache;
        this.cacheKey = cacheKey;
    }

    private enum LockType {
        READ,
        WRITE
    }
//
//    public int readData(byte[] outBuf, int cacheChunkIndexPos, int cacheChunkBytePos, int bufferBytePos) throws InterruptedException, IOException {
//        //first we try to acquire the read lock
//        acquireExclusiveRead(LOCK_TIMEOUT);
//
//        int byteCopied = 0;
//
//        //then we get the index to know where we are in the writes
//        EhcacheStreamMaster currentStreamMaster = getMasterIndexValue();
//        if(null != currentStreamMaster && cacheChunkIndexPos < currentStreamMaster.getNumberOfChunk()) {
//            //get chunk from cache
//            EhcacheStreamValue cacheChunkValue = getChunkValue(cacheChunkIndexPos);
//            if (null != cacheChunkValue && null != cacheChunkValue.getChunk()) {
//                byte[] cacheChunk = cacheChunkValue.getChunk();
//
//                //calculate the number of bytes to copy from the cacheChunks into the destination buffer based on the buffer size that's available
//                if (cacheChunk.length - cacheChunkBytePos < outBuf.length - bufferBytePos) {
//                    byteCopied = cacheChunk.length - cacheChunkBytePos;
//                } else {
//                    byteCopied = outBuf.length - bufferBytePos;
//                }
//
//                System.arraycopy(cacheChunk, cacheChunkBytePos, outBuf, bufferBytePos, byteCopied);
//            }
//        }
//
//        return byteCopied;
//    }
//
//    public DataReadOutPut readData2(byte[] outBuf, int cacheChunkIndexPos, int cacheChunkBytePos, int bufferBytePos) throws InterruptedException, IOException {
//        DataReadOutPut dataReadOutPut = null;
//
//        //first we try to acquire the read lock
//        acquireExclusiveRead(LOCK_TIMEOUT);
//
//        //then we get the index to know where we are in the writes
//        EhcacheStreamMaster currentStreamMaster = getMasterIndexValue();
//        if(null != currentStreamMaster && cacheChunkIndexPos < currentStreamMaster.getNumberOfChunk()){
//            //prepare the output for state maintaining
//            dataReadOutPut = new DataReadOutPut();
//
//            //get chunk from cache
//            EhcacheStreamValue cacheChunkValue = getChunkValue(cacheChunkIndexPos);
//            if(null != cacheChunkValue && null != cacheChunkValue.getChunk()) {
//                byte[] cacheChunk = cacheChunkValue.getChunk();
//
//                //calculate the number of bytes to copy from the cacheChunks into the destination buffer based on the buffer size that's available
//                int byteLengthToCopy;
//                if(cacheChunk.length - cacheChunkBytePos < outBuf.length - bufferBytePos){
//                    byteLengthToCopy = cacheChunk.length - cacheChunkBytePos;
//                } else {
//                    byteLengthToCopy = outBuf.length - bufferBytePos;
//                }
//
//                System.arraycopy(cacheChunk, cacheChunkBytePos, outBuf, bufferBytePos, byteLengthToCopy);
//
//                if (byteLengthToCopy > 0)
//                    dataReadOutPut.bufferAvailableBytes = bufferBytePos + byteLengthToCopy;
//
//                //track the chunk offset for next
//                if(byteLengthToCopy < cacheChunk.length - cacheChunkBytePos) {
//                    dataReadOutPut.cacheChunkIndexPos = cacheChunkIndexPos;
//                    dataReadOutPut.cacheChunkBytePos = cacheChunkBytePos + byteLengthToCopy;
//                } else { // it means we'll need to use the next chunk
//                    dataReadOutPut.cacheChunkIndexPos = cacheChunkIndexPos++;
//                    dataReadOutPut.cacheChunkBytePos = 0;
//                }
//            } else {
//                //this should not happen within the cacheValueTotalChunks boundaries...hence exception
//                throw new IOException("Cache chunk [" + (cacheChunkIndexPos) + "] is null and should not be since we're within the cache total chunks [=" +  currentStreamMaster.getNumberOfChunk() + "] boundaries.");
//            }
//        }
//
//        return dataReadOutPut;
//    }
//
//    public class DataReadOutPut{
//        private int cacheChunkIndexPos;
//        private int cacheChunkBytePos;
//        private int bufferAvailableBytes;
//
//        public int getCacheChunkIndexPos() {
//            return cacheChunkIndexPos;
//        }
//
//        public int getCacheChunkBytePos() {
//            return cacheChunkBytePos;
//        }
//
//        public int getBufferAvailableBytes() {
//            return bufferAvailableBytes;
//        }
//    }

    boolean acquireExclusiveRead(long timeout) throws InterruptedException {
        return tryLockInternal(buildMasterKey(),LockType.READ,timeout);
    }

    boolean acquireExclusiveWrite(long timeout) throws InterruptedException {
        return tryLockInternal(buildMasterKey(),LockType.WRITE,timeout);
    }

    void releaseExclusiveRead(){
        releaseLockInternal(buildMasterKey(),LockType.READ);
    }

    void releaseExclusiveWrite(){
        releaseLockInternal(buildMasterKey(),LockType.WRITE);
    }

    private boolean tryLockInternal(Object lockKey, LockType lockType, long timeout) throws InterruptedException {
        boolean isLocked = false;
        if(lockType == LockType.READ)
            isLocked = cache.tryReadLockOnKey(lockKey, timeout);
        else if(lockType == LockType.WRITE)
            isLocked = cache.tryWriteLockOnKey(lockKey, timeout);
        else
            throw new IllegalArgumentException("LockType not supported");

        if (isLocked) {
            return true;
        }
        return false;
    }

    private void releaseLockInternal(Object lockKey, LockType lockType) {
        if(lockType == LockType.READ)
            cache.releaseReadLockOnKey(lockKey);
        else if(lockType == LockType.WRITE)
            cache.releaseWriteLockOnKey(lockKey);
        else
            throw new IllegalArgumentException("LockType not supported");
    }

    EhcacheStreamMaster getMasterIndexValue(){
        EhcacheStreamMaster cacheMasterIndexValue = null;
        Element masterIndexElement = null;
        if(null != (masterIndexElement = getMasterIndexElement())) {
            cacheMasterIndexValue = (EhcacheStreamMaster)masterIndexElement.getObjectValue();
        }

        return cacheMasterIndexValue;
    }

    EhcacheStreamValue getChunkValue(int chunkIndex){
        EhcacheStreamValue chunkValue = null;
        Element chunkElem;
        if(null != (chunkElem = getChunkElement(chunkIndex)))
            chunkValue = (EhcacheStreamValue)chunkElem.getObjectValue();

        return chunkValue;
    }

    void putChunkValue(int chunkIndex, byte[] chunkPayload){
        cache.put(new Element(new EhcacheStreamKey(cacheKey, chunkIndex), new EhcacheStreamValue(chunkPayload)));
    }

    //clear all the chunks for this key...
    //for now, since we really don't know how many chunks keys are there, simple looping on 10,000 first combinations
    //maybe it'd be best to loop through all the keys and delete the needed ones...
    void clearAllChunks() {
        //remove all the chunk entries
        for(int i = 0; i < 10000; i++){
            cache.remove(new EhcacheStreamKey(cacheKey, i));
        }
    }

    boolean clearChunksForKey(EhcacheStreamMaster ehcacheStreamMasterIndex) {
        boolean success = false;
        if(null != ehcacheStreamMasterIndex){
            //remove all the chunk entries
            for(int i = 0; i < ehcacheStreamMasterIndex.getNumberOfChunk(); i++){
                cache.remove(new EhcacheStreamKey(cacheKey, i));
            }
            success = true;
        }
        return success;
    }

    private EhcacheStreamKey buildMasterKey(){
        return buildChunkKey(EhcacheStreamKey.MASTER_INDEX);
    }

    private EhcacheStreamKey buildChunkKey(int chunkIndex){
        return new EhcacheStreamKey(cacheKey, chunkIndex);
    }

    private Element getMasterIndexElement() {
        return cache.get(buildMasterKey());
    }

    private Element getChunkElement(int chunkIndex) {
        return cache.get(buildChunkKey(chunkIndex));
    }


//
//    EhcacheStreamMasterIndex getMasterIndexValueIfAvailable() throws IOException {
//        return getMasterIndexValueIfAvailable(getMasterIndexValue());
//    }
//
//    EhcacheStreamMasterIndex getMasterIndexValueIfAvailable(EhcacheStreamMasterIndex cacheMasterIndexValue) throws IOException {
//        return getMasterIndexValueIfAvailable(cacheMasterIndexValue, true);
//    }
//
//    EhcacheStreamMasterIndex getMasterIndexValueIfAvailable(EhcacheStreamMasterIndex cacheMasterIndexValue, boolean failIfCurrentWrite) throws IOException {
//        if(failIfCurrentWrite && null != cacheMasterIndexValue && cacheMasterIndexValue.isCurrentWrite())
//            throw new IOException("Operation not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");
//
//        return cacheMasterIndexValue;
//    }


    /**
     * Perform a CAS operation on the "critical" MasterIndex object
     * Replace the cached element only if the current Element is equal to the supplied old Element.
     *
     * @param      currentEhcacheStreamMaster   the current MasterIndex object that should be in cache
     * @param      newEhcacheStreamMaster  the new MasterIndex object to put in cache
     * @return     true if the Element was replaced
     *
     */
    boolean replaceEhcacheStreamMaster(EhcacheStreamMaster currentEhcacheStreamMaster, EhcacheStreamMaster newEhcacheStreamMaster) {
        boolean returnValue = false;
        if(null != currentEhcacheStreamMaster) {
            if(null != newEhcacheStreamMaster) {
                //replace old writeable element with new one using CAS operation for consistency
                returnValue = cache.replace(new Element(buildMasterKey(),currentEhcacheStreamMaster) , new Element(buildMasterKey(), newEhcacheStreamMaster));
            } else { // if null, let's understand this as a remove of current cache value
                returnValue = cache.removeElement(new Element(buildMasterKey(),currentEhcacheStreamMaster));
            }
        } else {
            if (null != newEhcacheStreamMaster) { //only add a new entry if the object to add is not null...otherwise do nothing
                Element previousElement = cache.putIfAbsent(new Element(buildMasterKey(), newEhcacheStreamMaster));
                returnValue = (previousElement == null);
            }
        }

        return returnValue;
    }

    /**
     * Perform a CAS operation on the MasterIndex object
     * Replace the cached element only if an Element is currently cached for this key
     *
     * @param      newEhcacheStreamMaster  the new MasterIndex object to put in cache
     * @return     The Element previously cached for this key, or null if no Element was cached
     *
     */
    Element replaceEhcacheStreamMaster(EhcacheStreamMaster newEhcacheStreamMaster) {
        //replace old writeable element with new one using CAS operation for consistency
        return cache.replace(new Element(buildMasterKey(), newEhcacheStreamMaster));
    }
}