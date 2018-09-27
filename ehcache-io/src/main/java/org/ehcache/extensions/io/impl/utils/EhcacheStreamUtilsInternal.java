package org.ehcache.extensions.io.impl.utils;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamKey;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.model.EhcacheStreamValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by fabien.sanglier on 8/2/18.
 */
public class EhcacheStreamUtilsInternal {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamUtilsInternal.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    /*
     * The Internal Ehcache cache object
     */
    final Ehcache cache;

    public EhcacheStreamUtilsInternal(Ehcache cache) {
        this.cache = cache;
    }

    public Ehcache getCache() {
        return cache;
    }

    public enum LockType {
        READ,
        WRITE
    }

    //Main CAS loop util method used by the CAS readers/writers
    public EhcacheStreamMaster atomicMutateEhcacheStreamMasterInCache(final Object cacheKey, final long timeoutMillis, final EhcacheStreamMaster.ComparatorType comparatorType, final EhcacheStreamMaster.MutationField mutationField, final EhcacheStreamMaster.MutationType mutationType, WaitStrategy waitStrategy) throws EhcacheStreamTimeoutException {
        EhcacheStreamMaster mutatedStreamMaster = null;
        long t1 = System.currentTimeMillis();
        long t2 = t1; //this ensures that the while always happen at least once!
        boolean isMutated = false;
        long attempts = 0L;
        while (!isMutated && t2 - t1 <= timeoutMillis) {
            //get the master index from cache, unless override is set
            EhcacheStreamMaster initialStreamMasterFromCache = getStreamMasterFromCache(cacheKey);

            if(comparatorType.check(initialStreamMasterFromCache)) {
                if(null == initialStreamMasterFromCache) {
                    mutatedStreamMaster = new EhcacheStreamMaster();
                } else {
                    mutatedStreamMaster = EhcacheStreamMaster.deepCopy(initialStreamMasterFromCache);
                }

                //mutation as requested
                mutationField.mutate(mutatedStreamMaster, mutationType);

                //concurrency check with CAS: let's save the initial EhcacheStreamMaster in cache, while making sure it hasn't change so far
                //if multiple threads are trying to do this replace on same key, only one thread is guaranteed to succeed here...while others will fail their CAS ops...and spin back to try again later.
                boolean replaced = replaceIfEqualEhcacheStreamMaster(cacheKey, initialStreamMasterFromCache, mutatedStreamMaster);
                if (replaced) {
                    if(isDebug)
                        logger.debug("Mutated object CAS committed in cache: {}", mutatedStreamMaster.toString());

                    //at this point, the object has been changed in cache as expected
                    isMutated = true;
                }
            }

            if(!isMutated) {
                waitStrategy.doWait(attempts); //wait time
                t2 = System.currentTimeMillis();
                attempts++;
            }
        }

        if(isDebug)
            logger.debug("Total cas loop iterations: {}", attempts);

        //if it's not mutated at the end of all the tries and timeout, throw timeout exception
        if (!isMutated) {
            throw new EhcacheStreamTimeoutException(String.format("Could not perform operation within %d internal retries totalling %d ms (timeout triggers at %d ms)", attempts, t2 - t1, timeoutMillis));
        }

        return mutatedStreamMaster;
    }

    public boolean atomicRemoveEhcacheStreamMasterInCache(final Object cacheKey, final long timeoutMillis) throws EhcacheStreamIllegalStateException, EhcacheStreamTimeoutException {
        return atomicRemoveEhcacheStreamMasterInCache(cacheKey, timeoutMillis, PropertyUtils.defaultWritesCasBackoffWaitStrategy);
    }

    //An atomic removal of a master stream entry + its related chunk entries.
    //Return TRUE for success state... otherwise throws an exception.
    public boolean atomicRemoveEhcacheStreamMasterInCache(final Object cacheKey, final long timeoutMillis, WaitStrategy waitStrategy) throws EhcacheStreamIllegalStateException, EhcacheStreamTimeoutException {
        EhcacheStreamMaster removedStreamMasterFromCache = null;
        long t1 = System.currentTimeMillis();
        long t2 = t1; //this ensures that the while always happen at least once!
        boolean isRemoved = false;
        long attempts = 0L;

        //first, mutate to WRITE mode to protect against concurrent Writes or READs
        EhcacheStreamMaster activeStreamMaster = atomicMutateEhcacheStreamMasterInCache(
                cacheKey,
                timeoutMillis,
                EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                EhcacheStreamMaster.MutationField.WRITERS,
                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW,
                waitStrategy
        );

        //If successful, CAS remove. Comparator for this CAS loop should be SINGLE WRITER
        final EhcacheStreamMaster.ComparatorType comparatorType = EhcacheStreamMaster.ComparatorType.SINGLE_WRITER;
        while (!isRemoved && t2 - t1 <= timeoutMillis) {
            //get the master index from cache, unless override is set
            removedStreamMasterFromCache = getStreamMasterFromCache(cacheKey);

            if(comparatorType.check(removedStreamMasterFromCache)) {
                //CAS remove stream master from cache (this op is the most important for consistency)
                boolean removed = removeIfPresentEhcacheStreamMaster(cacheKey, removedStreamMasterFromCache);
                if(removed) {
                    if(isDebug)
                        logger.debug("CAS removed object from cache: {}", removedStreamMasterFromCache.toString());

                    //clear related chunks
                    clearChunksFromStreamMaster(cacheKey, removedStreamMasterFromCache);

                    //at this point, the object has been changed in cache as expected
                    isRemoved = true;
                }
            }

            if(!isRemoved) {
                waitStrategy.doWait(attempts); //wait time
                t2 = System.currentTimeMillis();
                attempts++;
            }
        }

        if(isDebug)
            logger.debug("Total cas loop iterations: {}", attempts);

        //if it's not mutated at the end of all the tries and timeout, throw timeout exception
        if (!isRemoved) {
            throw new EhcacheStreamTimeoutException(String.format("Could not remove cache entry within %d ms (timeout triggers at %d ms)", t2 - t1, timeoutMillis));
        } else {
            //check that the master entry is actually removed
            if(null != getStreamMasterFromCache(cacheKey))
                throw new EhcacheStreamIllegalStateException("Master Entry was not removed as expected");

            //check that the other chunks are also removed
            EhcacheStreamValue[] chunkValues = getStreamChunksFromStreamMaster(cacheKey, removedStreamMasterFromCache);
            if(null != chunkValues && chunkValues.length > 0)
                throw new EhcacheStreamIllegalStateException("Some chunk entries were not removed as expected");
        }

        return isRemoved;
    }

    public boolean removeStreamEntryWithExplicitLocks(final Object cacheKey, long timeout) throws EhcacheStreamException {
        boolean removed = false;
        try {
            acquireExclusiveWriteOnMaster(cacheKey, timeout);

            //get stream master before removal
            EhcacheStreamMaster ehcacheStreamMaster = getStreamMasterFromCache(cacheKey);

            //remove stream master from cache (this op is the most important for consistency)
            if(null != ehcacheStreamMaster) {
                removed = removeIfPresentEhcacheStreamMaster(cacheKey, ehcacheStreamMaster);

                // if success removal, clean up the chunks...
                // if that fails it's not good for space usage, but data will still be inconsistent.
                // and we'll catch this issue in the next verification steps...
                if (removed)
                    clearChunksFromStreamMaster(cacheKey, ehcacheStreamMaster);
            } else {
                removed = true;
            }

            //check that the master entry is actually removed
            if(null != getStreamMasterFromCache(cacheKey))
                throw new EhcacheStreamException("Master Entry was not removed as expected");

            //check that the other chunks are also removed
            EhcacheStreamValue[] chunkValues = getStreamChunksFromStreamMaster(cacheKey, ehcacheStreamMaster);
            if(null != chunkValues && chunkValues.length > 0)
                throw new EhcacheStreamException("Some chunk entries were not removed as expected");
        } finally {
            releaseExclusiveWriteOnMaster(cacheKey);
        }

        return removed;
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
            List keys = new ArrayList<>(ehcacheStreamMasterIndex.getChunkCount());
            for(int i = 0; i < ehcacheStreamMasterIndex.getChunkCount(); i++){
                keys.add(new EhcacheStreamKey(cacheKey, i));
            }

            //actual removal
            cache.removeAll(keys);
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

