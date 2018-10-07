package org.ehcache.extensions.io.impl.utils;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamChunk;
import org.ehcache.extensions.io.impl.model.EhcacheStreamChunkKey;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMasterKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by fabien.sanglier on 8/2/18.
 */
public class EhcacheStreamUtilsInternal {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamUtilsInternal.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    private EhcacheStreamUtilsInternalImpl ehcacheStreamUtilsInternalImpl;

    public EhcacheStreamUtilsInternal(Ehcache cache) {
        this.ehcacheStreamUtilsInternalImpl = new EhcacheStreamUtilsInternalImpl(cache);
    }

    private enum LockType {
        READ,
        WRITE
    }

    public static final String toStringSafe(Object obj){
        return (null != obj)?obj.toString():"null";
    }

    private static EhcacheStreamMasterKey buildStreamMasterKey(final Object cacheKey) {
        return new EhcacheStreamMasterKey(cacheKey);
    }

    private static EhcacheStreamChunkKey buildStreamChunkKey(final Object cacheKey, int chunkIndex) {
        return new EhcacheStreamChunkKey(cacheKey, chunkIndex);
    }

    private static EhcacheStreamChunk buildStreamChunkValue(final byte[] bytes) {
        return new EhcacheStreamChunk(bytes);
    }

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    //////// Begin Public Accessor Section /////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////

    public EhcacheStreamMaster getStreamMasterFromCache(final Object publicCacheKey){
        return ehcacheStreamUtilsInternalImpl.getStreamMasterFromCache(
                buildStreamMasterKey(publicCacheKey)
        );
    }

    public EhcacheStreamMaster openWriteOnMaster(final Object publicCacheKey, final long timeoutMillis) throws EhcacheStreamTimeoutException {
        return ehcacheStreamUtilsInternalImpl.openWriteOnMaster(
                buildStreamMasterKey(publicCacheKey),
                timeoutMillis,
                PropertyUtils.defaultWritesCasBackoffWaitStrategy
        );
    }

    public EhcacheStreamMaster closeWriteOnMaster(final Object publicCacheKey, final long timeoutMillis) throws EhcacheStreamTimeoutException {
        return ehcacheStreamUtilsInternalImpl.closeWriteOnMaster(
                buildStreamMasterKey(publicCacheKey),
                timeoutMillis,
                PropertyUtils.defaultWritesCasBackoffWaitStrategy
        );
    }

    public EhcacheStreamMaster openReadOnMaster(final Object publicCacheKey, final long timeoutMillis) throws EhcacheStreamTimeoutException {
        return ehcacheStreamUtilsInternalImpl.openReadOnMaster(
                buildStreamMasterKey(publicCacheKey),
                timeoutMillis,
                PropertyUtils.defaultReadsCasBackoffWaitStrategy
        );
    }

    public EhcacheStreamMaster openSilentReadOnMaster(final Object publicCacheKey, final long timeoutMillis) throws EhcacheStreamTimeoutException {
        return ehcacheStreamUtilsInternalImpl.openSilentReadOnMaster(
                buildStreamMasterKey(publicCacheKey),
                timeoutMillis,
                PropertyUtils.defaultReadsCasBackoffWaitStrategy
        );
    }

    public EhcacheStreamMaster closeReadOnMaster(final Object publicCacheKey, final long timeoutMillis) throws EhcacheStreamTimeoutException {
        return ehcacheStreamUtilsInternalImpl.closeReadOnMaster(
                buildStreamMasterKey(publicCacheKey),
                timeoutMillis,
                PropertyUtils.defaultReadsCasBackoffWaitStrategy
        );
    }

    public boolean removeEhcacheStream(final Object publicCacheKey, final long timeoutMillis) throws EhcacheStreamIllegalStateException, EhcacheStreamTimeoutException {
        return ehcacheStreamUtilsInternalImpl.atomicRemoveEhcacheStreamMasterInCache(
                buildStreamMasterKey(publicCacheKey),
                timeoutMillis,
                PropertyUtils.defaultWritesCasBackoffWaitStrategy);
    }

    public boolean removeEhcacheStreamExplicitLocks(final Object publicCacheKey, long timeout) throws EhcacheStreamException {
        return ehcacheStreamUtilsInternalImpl.atomicRemoveEhcacheStreamMasterInCacheExplicitLocks(buildStreamMasterKey(publicCacheKey), timeout);
    }

    public void clearChunksFromStreamMaster(final Object publicCacheKey, EhcacheStreamMaster ehcacheStreamMaster) {
        ehcacheStreamUtilsInternalImpl.clearChunksFromStreamMaster(buildStreamMasterKey(publicCacheKey), ehcacheStreamMaster);
    }

    public boolean replaceIfPresentEhcacheStreamMaster(final Object publicCacheKey, EhcacheStreamMaster newEhcacheStreamMaster) {
        return ehcacheStreamUtilsInternalImpl.replaceIfPresentEhcacheStreamMaster(buildStreamMasterKey(publicCacheKey), newEhcacheStreamMaster);
    }

    public void putChunkValue(final Object publicCacheKey, int chunkIndex, byte[] chunk) throws CacheException {
        ehcacheStreamUtilsInternalImpl.putChunk(buildStreamChunkKey(publicCacheKey, chunkIndex), buildStreamChunkValue(chunk));
    }

    public EhcacheStreamChunk getChunkValue(final Object publicCacheKey, int chunkIndex){
        return ehcacheStreamUtilsInternalImpl.getChunkValue(buildStreamChunkKey(publicCacheKey, chunkIndex));
    }

    public void acquireExclusiveWriteOnMaster(final Object publicCacheKey, long timeout) throws EhcacheStreamTimeoutException, EhcacheStreamIllegalStateException {
        ehcacheStreamUtilsInternalImpl.acquireExclusiveWriteOnMaster(buildStreamMasterKey(publicCacheKey), timeout);
    }

    public void releaseExclusiveWriteOnMaster(final Object publicCacheKey) {
        ehcacheStreamUtilsInternalImpl.releaseExclusiveWriteOnMaster(buildStreamMasterKey(publicCacheKey));
    }

    public void acquireReadOnMaster(final Object publicCacheKey, long timeout) throws EhcacheStreamTimeoutException, EhcacheStreamIllegalStateException {
        ehcacheStreamUtilsInternalImpl.acquireReadOnMaster(buildStreamMasterKey(publicCacheKey), timeout);
    }

    public void releaseReadOnMaster(final Object publicCacheKey){
        ehcacheStreamUtilsInternalImpl.releaseLockInternal(buildStreamMasterKey(publicCacheKey), LockType.READ);
    }

    public List getAllStreamMasterPublicKeys(boolean checkForExpiry){
        return ehcacheStreamUtilsInternalImpl.getAllStreamMasterPublicKeys(checkForExpiry);
    }

    public List getAllStreamMasterPublicKeys(boolean checkForExpiry, boolean includeNoReads, boolean includeNoWrites, boolean includeReadsOnly, boolean includeWritesOnly){
        return ehcacheStreamUtilsInternalImpl.getAllStreamMasterPublicKeys(checkForExpiry, includeNoReads, includeNoWrites, includeReadsOnly, includeWritesOnly);
    }

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    //////// End Public Accessor Section ///////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////

    private class EhcacheStreamUtilsInternalImpl {
        /*
             * The Internal Ehcache cache object
             */
        final Ehcache cache;

        public EhcacheStreamUtilsInternalImpl(Ehcache cache) {
            this.cache = cache;
        }

        public Ehcache getCache() {
            return cache;
        }

        List getKeysWithExpiryCheck(){
            return cache.getKeysWithExpiryCheck();
        }

        List getKeys(){
            return cache.getKeys();
        }

        List getAllStreamMasterPublicKeys(boolean checkForExpiry){
            return getAllStreamMasterPublicKeys(checkForExpiry, true, true, true, true);
        }

        List getAllStreamMasterPublicKeys(boolean checkForExpiry, boolean includeNoReads, boolean includeNoWrites, boolean includeReadsOnly, boolean includeWritesOnly){
            List publicKeys;
            List internalKeys = (checkForExpiry)?getKeysWithExpiryCheck():getKeys();

            if(null != internalKeys && internalKeys.size() > 0) {
                publicKeys = new ArrayList(internalKeys.size());
                Iterator it = internalKeys.iterator();

                //if all true or all false, return all keys
                if(includeNoReads && includeNoWrites && includeWritesOnly && includeReadsOnly
                        || !includeNoReads && !includeNoWrites && !includeWritesOnly && !includeReadsOnly) {
                    while (it.hasNext()) {
                        Object internalKey = it.next();
                        if (null != internalKey
                                && internalKey.getClass().equals(EhcacheStreamMasterKey.class)) {
                            EhcacheStreamMasterKey ehcacheStreamMasterKey = (EhcacheStreamMasterKey) internalKey;
                            publicKeys.add(ehcacheStreamMasterKey.getCacheKey());
                        }
                    }
                } else {
                    while (it.hasNext()) {
                        Object internalKey = it.next();
                        if (null != internalKey
                                && internalKey.getClass().equals(EhcacheStreamMasterKey.class)) {

                            EhcacheStreamMasterKey ehcacheStreamMasterKey = (EhcacheStreamMasterKey) internalKey;
                            EhcacheStreamMaster ehcacheStreamMaster = getStreamMasterFromCache(ehcacheStreamMasterKey);
                            if (
                                    includeNoReads && ehcacheStreamMaster.getReaders() == 0 ||
                                            includeNoWrites && ehcacheStreamMaster.getWriters() == 0 ||
                                            includeWritesOnly && ehcacheStreamMaster.getWriters() > 0 ||
                                            includeReadsOnly && ehcacheStreamMaster.getReaders() > 0
                                    ) {
                                publicKeys.add(ehcacheStreamMasterKey.getCacheKey());
                            }
                        }
                    }
                }
            } else {
                publicKeys = Collections.emptyList();
            }

            return publicKeys;
        }

        EhcacheStreamMaster openWriteOnMaster(final EhcacheStreamMasterKey internalKey, final long timeoutMillis, WaitStrategy waitStrategy) throws EhcacheStreamTimeoutException {
            return atomicMutateEhcacheStreamMasterInCache(
                    internalKey,
                    timeoutMillis,
                    false,
                    EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                    EhcacheStreamMaster.MutationField.WRITERS,
                    EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW,
                    waitStrategy
            );
        }

        EhcacheStreamMaster closeWriteOnMaster(final EhcacheStreamMasterKey internalKey, final long timeoutMillis, WaitStrategy waitStrategy) throws EhcacheStreamTimeoutException {
            return atomicMutateEhcacheStreamMasterInCache(
                    internalKey,
                    timeoutMillis,
                    false,
                    EhcacheStreamMaster.ComparatorType.SINGLE_WRITER,
                    EhcacheStreamMaster.MutationField.WRITERS,
                    EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW,
                    waitStrategy
            );
        }

        EhcacheStreamMaster openReadOnMaster(final EhcacheStreamMasterKey internalKey, final long timeoutMillis, WaitStrategy waitStrategy) throws EhcacheStreamTimeoutException {
            return atomicMutateEhcacheStreamMasterInCache(
                    internalKey,
                    timeoutMillis,
                    true,
                    EhcacheStreamMaster.ComparatorType.NO_WRITER,
                    EhcacheStreamMaster.MutationField.READERS,
                    EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW,
                    waitStrategy
            );
        }

        EhcacheStreamMaster openSilentReadOnMaster(final EhcacheStreamMasterKey internalKey, final long timeoutMillis, WaitStrategy waitStrategy) throws EhcacheStreamTimeoutException {
            return atomicMutateEhcacheStreamMasterInCache(
                    internalKey,
                    timeoutMillis,
                    true,
                    EhcacheStreamMaster.ComparatorType.NO_WRITER,
                    EhcacheStreamMaster.MutationField.READERS,
                    EhcacheStreamMaster.MutationType.NONE,   //here, on purpose, we don't want to increment anything...kind of a silent read so if there's a write, it will acquire its write
                    waitStrategy
            );
        }

        EhcacheStreamMaster closeReadOnMaster(final EhcacheStreamMasterKey internalKey, final long timeoutMillis, WaitStrategy waitStrategy) throws EhcacheStreamTimeoutException {
            return atomicMutateEhcacheStreamMasterInCache(
                    internalKey,
                    timeoutMillis,
                    true,
                    EhcacheStreamMaster.ComparatorType.NO_WRITER,
                    EhcacheStreamMaster.MutationField.READERS,
                    EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW,
                    waitStrategy
            );
        }

        //Main CAS loop util method used by the CAS readers/writers
        EhcacheStreamMaster atomicMutateEhcacheStreamMasterInCache(final EhcacheStreamMasterKey internalKey, final long timeoutMillis, final boolean exitOnNullCacheEntry, final EhcacheStreamMaster.ComparatorType comparatorType, final EhcacheStreamMaster.MutationField mutationField, final EhcacheStreamMaster.MutationType mutationType, WaitStrategy waitStrategy) throws EhcacheStreamTimeoutException {
            EhcacheStreamMaster mutatedStreamMaster = null;
            boolean isMutated = false;
            long t1 = System.currentTimeMillis();
            long t2 = t1; //this ensures that the while always happen at least once!
            long attempts = 0L;

            while (!isMutated && t2 - t1 <= timeoutMillis) {
                //get the master index from cache, unless override is set
                EhcacheStreamMaster initialStreamMasterFromCache = getStreamMasterFromCache(internalKey);

                if(exitOnNullCacheEntry && null == initialStreamMasterFromCache){
                    isMutated = true;
                } else {
                    if (comparatorType.check(initialStreamMasterFromCache)) {
                        if (null == initialStreamMasterFromCache) {
                            mutatedStreamMaster = new EhcacheStreamMaster();
                        } else {
                            mutatedStreamMaster = EhcacheStreamMaster.deepCopy(initialStreamMasterFromCache);
                        }

                        //mutation as requested
                        mutationField.mutate(mutatedStreamMaster, mutationType);

                        //concurrency check with CAS: let's save the initial EhcacheStreamMaster in cache, while making sure it hasn't change so far
                        //if multiple threads are trying to do this replace on same key, only one thread is guaranteed to succeed here...while others will fail their CAS ops...and spin back to try again later.
                        isMutated = replaceIfEqualEhcacheStreamMaster(internalKey, initialStreamMasterFromCache, mutatedStreamMaster);
                    }
                }

                // loop control if not mutated
                if (!isMutated) {
                    waitStrategy.doWait(attempts); //wait time
                    attempts++;
                }
                t2 = System.currentTimeMillis();
            }

            //if it's not mutated at the end of all the tries and timeout, throw timeout exception
            if (!isMutated) {
                throw new EhcacheStreamTimeoutException(String.format(
                        "Could not perform Atomic mutate operation [%s,%s,%s] within [%d internal retries] totalling [%d ms] (timeout triggers at [%d ms]) - Key [%s]", toStringSafe(mutationField), toStringSafe(mutationType), toStringSafe(comparatorType), attempts, t2 - t1, timeoutMillis, toStringSafe(internalKey)));
            } else {
                if (isDebug) {
                    logger.debug(String.format(
                            "Successfully performed Atomic mutate operation [%s,%s,%s] within [%d internal retries] totalling [%d ms] (timeout triggers at [%d ms]) - Key [%s] / Returned Mutated Object [%s]", toStringSafe(mutationField), toStringSafe(mutationType), toStringSafe(comparatorType), attempts, t2 - t1, timeoutMillis, toStringSafe(internalKey), toStringSafe(mutatedStreamMaster)));
                }
            }

            return mutatedStreamMaster;
        }

        //An atomic removal of a master stream entry + its related chunk entries.
        //Return TRUE for success state... otherwise throws an exception.
        boolean atomicRemoveEhcacheStreamMasterInCache(final EhcacheStreamMasterKey ehcacheStreamMasterKey, final long timeoutMillis, WaitStrategy waitStrategy) throws EhcacheStreamIllegalStateException, EhcacheStreamTimeoutException {
            boolean isRemoved;

            //first, mutate to WRITE mode to protect against concurrent Writes or READs
            EhcacheStreamMaster activeStreamMaster = openWriteOnMaster(
                    ehcacheStreamMasterKey,
                    timeoutMillis,
                    waitStrategy
            );

            //CAS remove stream master from cache (this op is the most important for consistency)
            isRemoved = removeIfPresentEhcacheStreamMaster(ehcacheStreamMasterKey, activeStreamMaster);
            if (isRemoved) {
                if (isDebug)
                    logger.debug("Successful Atomic Remove operation for key {} / value {}", toStringSafe(ehcacheStreamMasterKey), toStringSafe(activeStreamMaster));

                //clear related chunks
                clearChunksFromStreamMaster(ehcacheStreamMasterKey, activeStreamMaster);

                if (isDebug)
                    logger.debug(String.format(
                            "Successfully removed all the chunks related to key {} / value {}", toStringSafe(ehcacheStreamMasterKey), toStringSafe(activeStreamMaster)));
            } else {
                //if it's not mutated at the end of all the tries and timeout, throw timeout exception
                throw new EhcacheStreamIllegalStateException(String.format(
                        "Could not perform Atomic Remove operation on key [%s]", toStringSafe(ehcacheStreamMasterKey)));
            }

            return isRemoved;
        }

        boolean atomicRemoveEhcacheStreamMasterInCacheExplicitLocks(final EhcacheStreamMasterKey ehcacheStreamMasterKey, long timeout) throws EhcacheStreamException {
            boolean removed = false;
            try {
                acquireExclusiveWriteOnMaster(ehcacheStreamMasterKey, timeout);

                //get stream master before removal
                EhcacheStreamMaster ehcacheStreamMaster = getStreamMasterFromCache(ehcacheStreamMasterKey);

                //remove stream master from cache (this op is the most important for consistency)
                if (null != ehcacheStreamMaster) {
                    removed = removeIfPresentEhcacheStreamMaster(ehcacheStreamMasterKey, ehcacheStreamMaster);

                    // if success removal, clean up the chunks...
                    // if that fails it's not good for space usage, but data will still be inconsistent.
                    // and we'll catch this issue in the next verification steps...
                    if (removed)
                        clearChunksFromStreamMaster(ehcacheStreamMasterKey, ehcacheStreamMaster);
                } else {
                    removed = true;
                }

                //check that the master entry is actually removed
                if (null != getStreamMasterFromCache(ehcacheStreamMasterKey))
                    throw new EhcacheStreamException("Master Entry was not removed as expected");

                //check that the other chunks are also removed
                EhcacheStreamChunk[] chunkValues = getStreamChunksFromStreamMaster(ehcacheStreamMasterKey, ehcacheStreamMaster);
                if (null != chunkValues && chunkValues.length > 0)
                    throw new EhcacheStreamException("Some chunk entries were not removed as expected");
            } finally {
                releaseExclusiveWriteOnMaster(ehcacheStreamMasterKey);
            }

            return removed;
        }

        void acquireReadOnMaster(final EhcacheStreamMasterKey ehcacheStreamMasterKey, long timeout) throws EhcacheStreamTimeoutException, EhcacheStreamIllegalStateException {
            try {
                boolean locked = tryLockInternal(ehcacheStreamMasterKey, LockType.READ, timeout);
                if (!locked) {
                    throw new EhcacheStreamTimeoutException(
                            String.format("Could not acquire an internal ehcache read lock on public key [%s] within timeout [%d ms]", toStringSafe(ehcacheStreamMasterKey), timeout)
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new EhcacheStreamIllegalStateException("Unexpected interrupt error: Could not acquire the internal ehcache read lock", e);
            }
        }

        void releaseReadOnMaster(final EhcacheStreamMasterKey ehcacheStreamMasterKey) {
            releaseLockInternal(ehcacheStreamMasterKey, LockType.READ);
        }

        void acquireExclusiveWriteOnMaster(final EhcacheStreamMasterKey ehcacheStreamMasterKey, long timeout) throws EhcacheStreamTimeoutException, EhcacheStreamIllegalStateException {
            try {
                boolean locked = tryLockInternal(ehcacheStreamMasterKey, LockType.WRITE, timeout);
                if (!locked) {
                    throw new EhcacheStreamTimeoutException(
                            String.format("Could not acquire an internal ehcache write lock on public key [%s] within timeout [%d ms]", toStringSafe(ehcacheStreamMasterKey), timeout)
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new EhcacheStreamIllegalStateException("Unexpected interrupt error: Could not acquire the internal ehcache write lock", e);
            }
        }

        void releaseExclusiveWriteOnMaster(final EhcacheStreamMasterKey ehcacheStreamMasterKey) {
            releaseLockInternal(ehcacheStreamMasterKey, LockType.WRITE);
        }

        // isReadLockedByCurrentThread throws a "UnsupportedOperationException Querying of read lock is not supported" for standalone ehcache...
        // fallback to non-query mode if we reach that
        synchronized boolean tryLockInternal(final EhcacheStreamMasterKey internalKey, final LockType lockType, final long timeout) throws InterruptedException {
            boolean isLocked = false;
            if (lockType == LockType.READ) {
                isLocked = cache.tryReadLockOnKey(internalKey, timeout);

                if (isDebug) {
                    if (isLocked)
                        logger.debug("Successfully acquired a internal ehcache read lock on internal key [{}] within timeout [{} ms]", toStringSafe(internalKey), timeout);
                    else
                        logger.debug("Did not acquire a internal ehcache read lock on internal key [{}] within timeout [{} ms]", toStringSafe(internalKey), timeout);
                }
            } else if (lockType == LockType.WRITE) {
                isLocked = cache.tryWriteLockOnKey(internalKey, timeout);

                if (isDebug) {
                    if (isLocked) {
                        logger.debug("Successfully acquired a internal ehcache write lock on internal key [{}] within timeout [{} ms]", toStringSafe(internalKey), timeout);
                    } else {
                        logger.debug("Did not acquire a internal ehcache write lock on internal key [{}] within timeout [{} ms]", toStringSafe(internalKey), timeout);
                    }
                }
            } else
                throw new IllegalArgumentException("LockType not supported");

            return isLocked;
        }

        // isReadLockedByCurrentThread throws a "UnsupportedOperationException Querying of read lock is not supported" for standalone ehcache...
        // fallback to non-query mode if we reach that
        synchronized void releaseLockInternal(final EhcacheStreamMasterKey internalKey, final LockType lockType) {
            if (lockType == LockType.READ) {
                cache.releaseReadLockOnKey(internalKey);
                if (isDebug)
                    logger.debug("Successfully released a internal ehcache read lock on internal key [{}]", toStringSafe(internalKey));
            } else if (lockType == LockType.WRITE) {
                cache.releaseWriteLockOnKey(internalKey);
                if (isDebug)
                    logger.debug("Successfully released a internal ehcache write lock on internal key [{}]", toStringSafe(internalKey));
            } else {
                throw new IllegalArgumentException("LockType not supported");
            }
        }

        ////////////// stream chunks operations

        Element buildChunkElement(final EhcacheStreamChunkKey internalKey, EhcacheStreamChunk ehcacheStreamChunk) {
            return new Element(internalKey, ehcacheStreamChunk);
        }

        Element getChunkElement(final EhcacheStreamChunkKey internalKey) throws CacheException {
            return cache.get(internalKey);
        }

        EhcacheStreamChunk getChunkValue(final EhcacheStreamChunkKey internalKey) {
            EhcacheStreamChunk chunkValue = null;
            Element chunkElem;
            if (null != (chunkElem = getChunkElement(internalKey)))
                chunkValue = (EhcacheStreamChunk) chunkElem.getObjectValue();

            return chunkValue;
        }

        void putChunk(final EhcacheStreamChunkKey internalKey, EhcacheStreamChunk internalValue) throws CacheException {
            cache.put(buildChunkElement(internalKey, internalValue));
        }

        boolean putChunkIfAbsent(final EhcacheStreamChunkKey internalKey, EhcacheStreamChunk internalValue) throws CacheException {
            Element previous = cache.putIfAbsent(buildChunkElement(internalKey, internalValue));
            return (previous != null);
        }

        EhcacheStreamChunk[] getStreamChunksFromStreamMasterKey(final EhcacheStreamMasterKey internalKey) {
            EhcacheStreamMaster ehcacheStreamMaster = getStreamMasterFromCache(internalKey);
            return getStreamChunksFromStreamMaster(internalKey, ehcacheStreamMaster);
        }

        EhcacheStreamChunk[] getStreamChunksFromStreamMaster(final EhcacheStreamMasterKey internalKey, final EhcacheStreamMaster ehcacheStreamMaster) {
            List chunkValues = null;
            if (null != ehcacheStreamMaster) {
                chunkValues = new ArrayList(ehcacheStreamMaster.getChunkCount());
                for (int i = 0; i < ehcacheStreamMaster.getChunkCount(); i++) {
                    EhcacheStreamChunk chunkValue = getChunkValue(new EhcacheStreamChunkKey(internalKey.getCacheKey(), i));
                    if (null != chunkValue)
                        chunkValues.add(chunkValue);
                }
            }

            if (null == chunkValues)
                chunkValues = Collections.emptyList();

            return (EhcacheStreamChunk[]) chunkValues.toArray(new EhcacheStreamChunk[chunkValues.size()]);
        }

        void clearChunksFromStreamMasterKey(final EhcacheStreamMasterKey internalKey) {
            EhcacheStreamMaster ehcacheStreamMaster = getStreamMasterFromCache(internalKey);
            clearChunksFromStreamMaster(internalKey, ehcacheStreamMaster);
        }

        void clearChunksFromStreamMaster(final EhcacheStreamMasterKey ehcacheStreamMasterKey, final EhcacheStreamMaster ehcacheStreamMaster) {
            if (null != ehcacheStreamMaster) {
                //remove all the chunk entries
                List keys = new ArrayList<>(ehcacheStreamMaster.getChunkCount());
                for (int i = 0; i < ehcacheStreamMaster.getChunkCount(); i++) {
                    keys.add(new EhcacheStreamChunkKey(ehcacheStreamMasterKey.getCacheKey(), i));
                }

                //actual removal
                cache.removeAll(keys);
            }
        }

        ////////////// stream master operations
        Element buildStreamMasterElement(final EhcacheStreamMasterKey internalKey, EhcacheStreamMaster ehcacheStreamMaster) {
            return new Element(internalKey, ehcacheStreamMaster);
        }

        Element getStreamMasterElement(final EhcacheStreamMasterKey internalKey) throws CacheException {
            return cache.get(internalKey);
        }

        EhcacheStreamMaster getStreamMasterFromCache(final EhcacheStreamMasterKey internalKey) {
            EhcacheStreamMaster cacheMasterIndexValue = null;
            Element masterIndexElement = null;
            if (null != (masterIndexElement = getStreamMasterElement(internalKey))) {
                cacheMasterIndexValue = (EhcacheStreamMaster) masterIndexElement.getObjectValue();
            }

            return cacheMasterIndexValue;
        }

        /**
         * Perform a CAS operation on the "critical" MasterIndex object
         * Replace the cached element only if the current Element is equal to the supplied old Element.
         *
         * @param oldEhcacheStreamMaster the old MasterIndex object to replace
         * @param newEhcacheStreamMaster the new MasterIndex object
         * @return true if the Element was replaced
         */
        boolean replaceIfEqualEhcacheStreamMaster(final EhcacheStreamMasterKey internalKey, final EhcacheStreamMaster oldEhcacheStreamMaster, final EhcacheStreamMaster newEhcacheStreamMaster) {
            boolean replaced = false;
            if (null != oldEhcacheStreamMaster) {
                //replace old writeable element with new one using CAS operation for consistency
                replaced = cache.replace(buildStreamMasterElement(internalKey, oldEhcacheStreamMaster), buildStreamMasterElement(internalKey, newEhcacheStreamMaster));
            } else {
                Element previousElement = cache.putIfAbsent(buildStreamMasterElement(internalKey, newEhcacheStreamMaster));
                replaced = (previousElement == null);
            }

            return replaced;
        }

        /**
         * Perform a CAS operation on the MasterIndex object
         * Replace the cached element only if an Element is currently cached for this key
         *
         * @param newEhcacheStreamMaster the new MasterIndex object to put in cache
         * @return The Element previously cached for this key, or null if no Element was cached
         */
        boolean replaceIfPresentEhcacheStreamMaster(final EhcacheStreamMasterKey internalKey, EhcacheStreamMaster newEhcacheStreamMaster) {
            //replace old writeable element with new one using CAS operation for consistency
            Element previous = cache.replace(buildStreamMasterElement(internalKey, newEhcacheStreamMaster));
            return (previous != null);
        }

        /**
         * Perform a CAS operation on the MasterIndex object
         * Replace the cached element only if an Element is currently cached for this key
         *
         * @param internalKey            the master cache key for this stream entry
         * @param oldEhcacheStreamMaster the new MasterIndex object to put in cache
         * @return The Element previously cached for this key, or null if no Element was cached
         */
        boolean removeIfPresentEhcacheStreamMaster(final EhcacheStreamMasterKey internalKey, EhcacheStreamMaster oldEhcacheStreamMaster) {
            return cache.removeElement(buildStreamMasterElement(internalKey, oldEhcacheStreamMaster));
        }
    }
}

