package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.ehcache.extensions.io.EhcacheStreamException;

/**
 * Created by fabien.sanglier on 8/2/18.
 */
public class EhcacheStreamUtils {
    /*
     * The Internal Ehcache cache object
     */
    final Cache cache;

    /*
     * The Ehcache cache key object the data should get written to
     */
    final Object cacheKey;

    public EhcacheStreamUtils(Cache cache, Object cacheKey) {
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

    public EhcacheStreamMaster getStreamMasterFromCache(){
        EhcacheStreamMaster cacheMasterIndexValue = null;
        Element masterIndexElement = null;
        if(null != (masterIndexElement = getStreamMasterElement())) {
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

    protected Element buildStreamMasterElement(EhcacheStreamMaster ehcacheStreamMaster) {
        return new Element(buildMasterKey(), ehcacheStreamMaster);
    }

    protected Element getStreamMasterElement() {
        return cache.get(buildMasterKey());
    }

    private Element getChunkElement(int chunkIndex) {
        return cache.get(buildChunkKey(chunkIndex));
    }

    public synchronized void removeEhcacheStreamEntry(long timeout) throws EhcacheStreamException {
        try {
            try {
                acquireExclusiveWriteOnMaster(timeout);
            } catch (InterruptedException e) {
                throw new EhcacheStreamException("Could not acquire the internal ehcache write lock", e);
            }

            EhcacheStreamMaster ehcacheStreamMaster = getStreamMasterFromCache();
            if(replaceEhcacheStreamMaster(ehcacheStreamMaster, null))
                clearChunksForKey(ehcacheStreamMaster);
        } finally {
            releaseExclusiveWriteOnMaster();
        }
    }

    /**
     * Perform a CAS operation on the "critical" MasterIndex object
     * Replace the cached element only if the current Element is equal to the supplied old Element.
     *
     * @param      oldEhcacheStreamMaster  the old MasterIndex object to replace
     * @param      newEhcacheStreamMaster  the new MasterIndex object
     * @return     true if the Element was replaced
     *
     */
    protected boolean replaceEhcacheStreamMaster(EhcacheStreamMaster oldEhcacheStreamMaster, EhcacheStreamMaster newEhcacheStreamMaster) {
        boolean returnValue = false;
        if(null != oldEhcacheStreamMaster) {
            if(null != newEhcacheStreamMaster) {
                //replace old writeable element with new one using CAS operation for consistency
                returnValue = cache.replace(buildStreamMasterElement(oldEhcacheStreamMaster) , buildStreamMasterElement(newEhcacheStreamMaster));
            } else { // if null, let's understand this as a remove of current cache value
                returnValue = cache.removeElement(buildStreamMasterElement(oldEhcacheStreamMaster));
            }
        } else {
            if (null != newEhcacheStreamMaster) { //only add a new entry if the object to add is not null...otherwise do nothing
                Element previousElement = cache.putIfAbsent(buildStreamMasterElement(newEhcacheStreamMaster));
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
        Element previous = cache.replace(buildStreamMasterElement(newEhcacheStreamMaster));
        return (previous != null);
    }
}

