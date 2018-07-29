package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public abstract class BaseEhcacheStream {
    public static final long LOCK_TIMEOUT = 10000;

    /*
     * The Internal Ehcache cache object
     */
    final Cache cache;

    /*
     * The Ehcache cache key object the data should get written to
     */
    final Object cacheKey;

    protected BaseEhcacheStream(Cache cache, Object cacheKey) {
        //TODO: we should check if the cache is not null but maybe enforce "Pinning"?? (because otherwise cache chunks can disappear and that would mess up the data consistency...)
        this.cache = cache;
        this.cacheKey = cacheKey;
    }

    protected Cache getCache() {
        return cache;
    }

    protected Object getCacheKey() {
        return cacheKey;
    }

    private enum LockType {
        READ,
        WRITE
    }

    protected boolean acquireReadOnMaster(long timeout) throws InterruptedException {
        return tryLockInternal(buildMasterKey(),LockType.READ,timeout);
    }

    protected void releaseReadOnMaster(){
        releaseLockInternal(buildMasterKey(),LockType.READ);
    }

    protected boolean acquireExclusiveWriteOnMaster(long timeout) throws InterruptedException {
        return tryLockInternal(buildMasterKey(),LockType.WRITE,timeout);
    }

    protected void releaseExclusiveWriteOnMaster(){
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

    protected EhcacheStreamMaster getMasterIndexValue(){
        EhcacheStreamMaster cacheMasterIndexValue = null;
        Element masterIndexElement = null;
        if(null != (masterIndexElement = getMasterIndexElement())) {
            cacheMasterIndexValue = (EhcacheStreamMaster)masterIndexElement.getObjectValue();
        }

        return cacheMasterIndexValue;
    }

    protected EhcacheStreamValue getChunkValue(int chunkIndex){
        EhcacheStreamValue chunkValue = null;
        Element chunkElem;
        if(null != (chunkElem = getChunkElement(chunkIndex)))
            chunkValue = (EhcacheStreamValue)chunkElem.getObjectValue();

        return chunkValue;
    }

    protected void putChunkValue(int chunkIndex, byte[] chunkPayload){
        cache.put(new Element(new EhcacheStreamKey(cacheKey, chunkIndex), new EhcacheStreamValue(chunkPayload)));
    }

    //clear all the chunks for this key...
    //for now, since we really don't know how many chunks keys are there, simple looping on 10,000 first combinations
    //maybe it'd be best to loop through all the keys and delete the needed ones...
    protected void clearAllChunks() {
        //remove all the chunk entries
        for(int i = 0; i < 10000; i++){
            cache.remove(new EhcacheStreamKey(cacheKey, i));
        }
    }

    protected void clearChunksForKey(EhcacheStreamMaster ehcacheStreamMasterIndex) {
        if(null != ehcacheStreamMasterIndex){
            //remove all the chunk entries
            for(int i = 0; i < ehcacheStreamMasterIndex.getChunkCounter(); i++){
                cache.remove(new EhcacheStreamKey(cacheKey, i));
            }
        }
    }

    private EhcacheStreamKey buildMasterKey(){
        return buildChunkKey(EhcacheStreamKey.MASTER_INDEX);
    }

    private EhcacheStreamKey buildChunkKey(int chunkIndex){
        return new EhcacheStreamKey(cacheKey, chunkIndex);
    }

    protected Element getMasterIndexElement() {
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
     * @param      currentEhcacheStreamMasterElement   the current MasterIndex object that should be in cache
     * @param      newEhcacheStreamMaster  the new MasterIndex object to put in cache
     * @return     true if the Element was replaced
     *
     */
    protected boolean replaceEhcacheStreamMaster(Element currentEhcacheStreamMasterElement, EhcacheStreamMaster newEhcacheStreamMaster) {
        boolean returnValue = false;
        if(null != currentEhcacheStreamMasterElement) {
            if(null != newEhcacheStreamMaster) {
                //replace old writeable element with new one using CAS operation for consistency
                returnValue = cache.replace(currentEhcacheStreamMasterElement , new Element(buildMasterKey(), newEhcacheStreamMaster));
            } else { // if null, let's understand this as a remove of current cache value
                returnValue = cache.removeElement(currentEhcacheStreamMasterElement);
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
    protected boolean replaceEhcacheStreamMaster(EhcacheStreamMaster newEhcacheStreamMaster) {
        //replace old writeable element with new one using CAS operation for consistency
        Element previous = cache.replace(new Element(buildMasterKey(), newEhcacheStreamMaster));
        return (previous != null);
    }
}