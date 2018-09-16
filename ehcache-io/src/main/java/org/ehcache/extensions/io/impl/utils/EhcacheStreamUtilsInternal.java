package org.ehcache.extensions.io.impl.utils;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamKey;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.model.EhcacheStreamValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by fabien.sanglier on 8/2/18.
 */
public class EhcacheStreamUtilsInternal {
    /*
     * The Internal Ehcache cache object
     */
    final Ehcache cache;

    public EhcacheStreamUtilsInternal(Ehcache cache) {
        //TODO: we should check if the cache is not null but maybe enforce "Pinning"?? (because otherwise cache chunks can disappear and that would mess up the data consistency...)
        this.cache = cache;
    }

    public Ehcache getCache() {
        return cache;
    }

    public enum LockType {
        READ,
        WRITE
    }

    public void acquireReadOnMaster(final Object cacheKey, long timeout) throws EhcacheStreamException {
        EhcacheStreamKey key = buildMasterKey(cacheKey);
        try {
            boolean locked = tryLockInternal(key,LockType.READ,timeout);
            if(!locked)
                throw new EhcacheStreamTimeoutException("Could not acquire the internal ehcache read lock on key [" + key.toString() + "] within timeout [" + timeout + "ms]");
        } catch (InterruptedException e) {
            throw new EhcacheStreamException("Unexpected interrupt error: Could not acquire the internal ehcache read lock", e);
        }
    }

    public void releaseReadOnMaster(final Object cacheKey){
        releaseLockInternal(buildMasterKey(cacheKey), LockType.READ);
    }

    public void acquireExclusiveWriteOnMaster(final Object cacheKey, long timeout) throws EhcacheStreamException {
        EhcacheStreamKey key = buildMasterKey(cacheKey);
        try {
            boolean locked = tryLockInternal(key, LockType.WRITE, timeout);
            if(!locked)
                throw new EhcacheStreamTimeoutException("Could not acquire the internal ehcache write lock on key [" + key.toString() + "] within timeout [" + timeout + "ms]");
        } catch (InterruptedException e) {
            throw new EhcacheStreamException("Unexpected interrupt error: Could not acquire the internal ehcache write lock", e);
        }
    }

    public void releaseExclusiveWriteOnMaster(final Object cacheKey){
        releaseLockInternal(buildMasterKey(cacheKey), LockType.WRITE);
    }

    // isReadLockedByCurrentThread throws a "UnsupportedOperationException Querying of read lock is not supported" for standalone ehcache...
    // fallback to non-query mode if we reach that
    private synchronized boolean tryLockInternal(Object lockKey, LockType lockType, long timeout) throws InterruptedException {
        boolean isLocked = false;
        if(lockType == LockType.READ) {
            try {
                isLocked = cache.isReadLockedByCurrentThread(lockKey) || cache.tryReadLockOnKey(lockKey, timeout);
            } catch (UnsupportedOperationException uex){
                isLocked = cache.tryReadLockOnKey(lockKey, timeout);
            }
        }
        else if(lockType == LockType.WRITE) {
            try {
                isLocked = cache.isWriteLockedByCurrentThread(lockKey) || cache.tryWriteLockOnKey(lockKey, timeout);
            } catch (UnsupportedOperationException uex){
                isLocked = cache.tryWriteLockOnKey(lockKey, timeout);
            }
        }
        else
            throw new IllegalArgumentException("LockType not supported");

        return isLocked;
    }

    // isReadLockedByCurrentThread throws a "UnsupportedOperationException Querying of read lock is not supported" for standalone ehcache...
    // fallback to non-query mode if we reach that
    public synchronized void releaseLockInternal(Object lockKey, LockType lockType) {
        if(lockType == LockType.READ) {
            try {
                if (cache.isReadLockedByCurrentThread(lockKey)) cache.releaseReadLockOnKey(lockKey);
            } catch (UnsupportedOperationException uex){
                cache.releaseReadLockOnKey(lockKey);
            }
        } else if(lockType == LockType.WRITE) {
            try {
                if(cache.isWriteLockedByCurrentThread(lockKey)) cache.releaseWriteLockOnKey(lockKey);
            } catch (UnsupportedOperationException uex){
                cache.releaseWriteLockOnKey(lockKey);
            }
        }
        else
            throw new IllegalArgumentException("LockType not supported");
    }

    public EhcacheStreamValue getChunkValue(final Object cacheKey, int chunkIndex){
        EhcacheStreamValue chunkValue = null;
        Element chunkElem;
        if(null != (chunkElem = getChunkElement(cacheKey, chunkIndex)))
            chunkValue = (EhcacheStreamValue)chunkElem.getObjectValue();

        return chunkValue;
    }

    public void putChunkValue(final Object cacheKey, int chunkIndex, byte[] chunkPayload) throws CacheException {
        cache.put(new Element(new EhcacheStreamKey(cacheKey, chunkIndex), new EhcacheStreamValue(chunkPayload)));
    }

    //CAS
    public boolean putChunkValueIfAbsent(final Object cacheKey, int chunkIndex, byte[] chunkPayload) throws CacheException {
        Element previous = cache.putIfAbsent(new Element(new EhcacheStreamKey(cacheKey, chunkIndex), new EhcacheStreamValue(chunkPayload)));
        return (previous != null);
    }

    private EhcacheStreamKey buildMasterKey(final Object cacheKey){
        return buildChunkKey(cacheKey, EhcacheStreamKey.MASTER_INDEX);
    }

    private EhcacheStreamKey buildChunkKey(final Object cacheKey, final int chunkIndex){
        return new EhcacheStreamKey(cacheKey, chunkIndex);
    }

    public Element buildStreamMasterElement(final Object cacheKey, EhcacheStreamMaster ehcacheStreamMaster) {
        return new Element(buildMasterKey(cacheKey), ehcacheStreamMaster);
    }

    public Element getStreamMasterElement(final Object cacheKey) throws CacheException {
        return cache.get(buildMasterKey(cacheKey));
    }

    private Element getChunkElement(final Object cacheKey, int chunkIndex) throws CacheException {
        return cache.get(buildChunkKey(cacheKey, chunkIndex));
    }

    public EhcacheStreamMaster getStreamMasterFromCache(final Object cacheKey){
        EhcacheStreamMaster cacheMasterIndexValue = null;
        Element masterIndexElement = null;
        if(null != (masterIndexElement = getStreamMasterElement(cacheKey))) {
            cacheMasterIndexValue = (EhcacheStreamMaster)masterIndexElement.getObjectValue();
        }

        return cacheMasterIndexValue;
    }

    public EhcacheStreamValue[] getStreamChunksFromCache(final Object cacheKey){
        return getStreamChunksFromStreamMaster(cacheKey, getStreamMasterFromCache(cacheKey));
    }

    public EhcacheStreamValue[] getStreamChunksFromStreamMaster(final Object cacheKey, final EhcacheStreamMaster ehcacheStreamMaster){
        List chunkValues = null;
        if(null != ehcacheStreamMaster){
            chunkValues = new ArrayList(ehcacheStreamMaster.getChunkCount());
            for(int i = 0; i < ehcacheStreamMaster.getChunkCount(); i++){
                EhcacheStreamValue chunkValue = getChunkValue(cacheKey, i);
                if(null != chunkValue)
                    chunkValues.add(chunkValue);
            }
        }

        if(null == chunkValues)
            chunkValues = Collections.emptyList();

        return (EhcacheStreamValue[])chunkValues.toArray(new EhcacheStreamValue[chunkValues.size()]);
    }

    public void clearChunksFromCacheKey(final Object cacheKey) {
        clearChunksFromStreamMaster(cacheKey, getStreamMasterFromCache(cacheKey));
    }

    public void clearChunksFromStreamMaster(final Object cacheKey, final EhcacheStreamMaster ehcacheStreamMasterIndex) {
        if(null != ehcacheStreamMasterIndex){
            //remove all the chunk entries
            for(int i = 0; i < ehcacheStreamMasterIndex.getChunkCount(); i++){
                cache.remove(new EhcacheStreamKey(cacheKey, i));
            }
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
    public boolean replaceIfEqualEhcacheStreamMaster(final Object cacheKey, EhcacheStreamMaster oldEhcacheStreamMaster, EhcacheStreamMaster newEhcacheStreamMaster) {
        boolean replaced = false;
        if(null != oldEhcacheStreamMaster) {
            //replace old writeable element with new one using CAS operation for consistency
            replaced = cache.replace(buildStreamMasterElement(cacheKey, oldEhcacheStreamMaster) , buildStreamMasterElement(cacheKey, newEhcacheStreamMaster));
        } else {
            Element previousElement = cache.putIfAbsent(buildStreamMasterElement(cacheKey, newEhcacheStreamMaster));
            replaced = (previousElement == null);
        }

        return replaced;
    }

    /**
     * Perform a CAS operation on the MasterIndex object
     * Replace the cached element only if an Element is currently cached for this key
     *
     * @param      newEhcacheStreamMaster  the new MasterIndex object to put in cache
     * @return     The Element previously cached for this key, or null if no Element was cached
     *
     */
    public boolean replaceIfPresentEhcacheStreamMaster(final Object cacheKey, EhcacheStreamMaster newEhcacheStreamMaster) {
        //replace old writeable element with new one using CAS operation for consistency
        Element previous = cache.replace(buildStreamMasterElement(cacheKey, newEhcacheStreamMaster));
        return (previous != null);
    }

    /**
     * Perform a CAS operation on the MasterIndex object
     * Replace the cached element only if an Element is currently cached for this key
     *
     * @param      cacheKey  the master cache key for this stream entry
     * @param      oldEhcacheStreamMaster  the new MasterIndex object to put in cache
     * @return     The Element previously cached for this key, or null if no Element was cached
     *
     */
    public boolean removeIfPresentEhcacheStreamMaster(final Object cacheKey, EhcacheStreamMaster oldEhcacheStreamMaster) {
        return cache.removeElement(buildStreamMasterElement(cacheKey, oldEhcacheStreamMaster));
    }
}

