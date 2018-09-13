package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by fabien.sanglier on 8/2/18.
 */
public class EhcacheStreamUtils {
    public static final String PROP_INPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.inputstream.buffersize";
    public static final String PROP_INPUTSTREAM_OPEN_TIMEOUTS = "ehcache.extension.io.inputstream.opentimeout";
    public static final String PROP_INPUTSTREAM_ALLOW_NULLSTREAM = "ehcache.extension.io.inputstream.allownull";

    public static final String PROP_OUTPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.outputstream.buffersize";
    public static final String PROP_OUTPUTSTREAM_OVERRIDE = "ehcache.extension.io.outputstream.override";
    public static final String PROP_OUTPUTSTREAM_OPEN_TIMEOUTS = "ehcache.extension.io.outputstream.opentimeout";


    public static final int DEFAULT_OUTPUTSTREAM_BUFFER_SIZE = 128 * 1024; // 128kb
    public static final boolean DEFAULT_OUTPUTSTREAM_OVERRIDE = true;
    public static final int DEFAULT_INPUTSTREAM_BUFFER_SIZE = 512 * 1024; // 512kb
    public static final long DEFAULT_OUTPUTSTREAM_OPEN_TIMEOUT = 10000L;
    public static final long DEFAULT_INPUTSTREAM_OPEN_TIMEOUT = 2000L;
    public static final boolean DEFAULT_INPUTSTREAM_ALLOW_NULLSTREAM = false;
    public static final boolean DEFAULT_RELEASELOCK_CHECKTHREAD_OWNERSHIP = true;

    /*
     * The Internal Ehcache cache object
     */
    final Ehcache cache;

    public EhcacheStreamUtils(Ehcache cache) {
        //TODO: we should check if the cache is not null but maybe enforce "Pinning"?? (because otherwise cache chunks can disappear and that would mess up the data consistency...)
        this.cache = cache;
    }

    protected Ehcache getCache() {
        return cache;
    }

    /////////////////////////////////
    ////   public accessors
    /////////////////////////////////

    /**
     * Remove a Stream entry from cache
     *
     * @param      cacheKey  the public cache key for this stream entry
     * @param      timeout   the timeout to acquire the write lock on that cachekey
     * @return     true if Stream entry was removed
     * @exception   EhcacheStreamException
     *
     */
    public synchronized boolean removeStreamEntry(final Object cacheKey, long timeout) throws EhcacheStreamException {
        boolean removed = false;
        try {
            acquireExclusiveWriteOnMaster(cacheKey, timeout);

            //get stream master before removal
            EhcacheStreamMaster ehcacheStreamMaster = getStreamMasterFromCache(cacheKey);

            //remove stream master from cache (this op is the most important for consistency)
            removed = removeIfPresentEhcacheStreamMaster(cacheKey, ehcacheStreamMaster);

            // if success removal, clean up the chunks...
            // if that fails it's not good for space usage, but data will still be inconsistent.
            // and we'll catch this issue in the next verification steps...
            if(removed)
                clearChunksFromStreamMaster(cacheKey, ehcacheStreamMaster);

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

    /**
     * Check if a Stream entry exist in cache
     *
     * @param      cacheKey  the public cache key for this stream entry
     * @return     true if Stream entry is in cache
     *
     */
    public boolean containsStreamEntry(final Object cacheKey) {
        return null != getStreamMasterFromCache(cacheKey);
    }

    /**
     * Get a list of all the public keys (the key objects used by the client apps) in cache
     *
     * @param       checkForExpiry        if true, returns only the keys that are not expired. NOTE: this could take much longer time if cache is large.
     * @return      A list of public key objects
     */
    public List getAllStreamEntryKeys(boolean checkForExpiry){
        List publicKeys;
        List internalKeys = (checkForExpiry)?cache.getKeysWithExpiryCheck():cache.getKeys();

        if(null != internalKeys && internalKeys.size() > 0) {
            publicKeys = new ArrayList(internalKeys.size());
            Iterator it = internalKeys.iterator();
            while( it.hasNext() ) {
                Object cacheKey = it.next();
                if( null != cacheKey
                        && cacheKey.getClass().equals(EhcacheStreamKey.class)
                        && ((EhcacheStreamKey)cacheKey).getChunkIndex() == EhcacheStreamKey.MASTER_INDEX){
                    publicKeys.add(((EhcacheStreamKey)cacheKey).getCacheKey());
                }
            }
        } else {
            publicKeys = Collections.emptyList();
        }

        return publicKeys;
    }

    /////////////////////////////////
    ////   End public accessors
    /////////////////////////////////

    private enum LockType {
        READ,
        WRITE
    }

    protected void acquireReadOnMaster(final Object cacheKey, long timeout) throws EhcacheStreamException {
        EhcacheStreamKey key = buildMasterKey(cacheKey);
        try {
            boolean locked = tryLockInternal(key,LockType.READ,timeout);
            if(!locked)
                throw new EhcacheStreamException("Could not acquire the internal ehcache read lock on key [" + key.toString() + "] within timeout [" + timeout + "ms]");
        } catch (InterruptedException e) {
            throw new EhcacheStreamException("Unexpected interrupt error: Could not acquire the internal ehcache read lock", e);
        }
    }

    protected void releaseReadOnMaster(final Object cacheKey){
        releaseReadOnMaster(cacheKey, EhcacheStreamUtils.DEFAULT_RELEASELOCK_CHECKTHREAD_OWNERSHIP);
    }

    protected void releaseReadOnMaster(final Object cacheKey, boolean checkOnlyForCurrentThread){
        releaseLockInternal(buildMasterKey(cacheKey),LockType.READ, checkOnlyForCurrentThread);
    }

    protected void acquireExclusiveWriteOnMaster(final Object cacheKey, long timeout) throws EhcacheStreamException {
        EhcacheStreamKey key = buildMasterKey(cacheKey);
        try {
            boolean locked = tryLockInternal(key, LockType.WRITE, timeout);
            if(!locked)
                throw new EhcacheStreamException("Could not acquire the internal ehcache write lock on key [" + key.toString() + "] within timeout [" + timeout + "ms]");
        } catch (InterruptedException e) {
            throw new EhcacheStreamException("Unexpected interrupt error: Could not acquire the internal ehcache write lock", e);
        }
    }

    protected void releaseExclusiveWriteOnMaster(final Object cacheKey){
        releaseExclusiveWriteOnMaster(cacheKey, EhcacheStreamUtils.DEFAULT_RELEASELOCK_CHECKTHREAD_OWNERSHIP);
    }

    protected void releaseExclusiveWriteOnMaster(final Object cacheKey, boolean checkOnlyForCurrentThread){
        releaseLockInternal(buildMasterKey(cacheKey),LockType.WRITE, checkOnlyForCurrentThread);
    }

    private synchronized boolean tryLockInternal(Object lockKey, LockType lockType, long timeout) throws InterruptedException {
        boolean isLocked = false;
        if(lockType == LockType.READ)
            isLocked = cache.tryReadLockOnKey(lockKey, timeout);
        else if(lockType == LockType.WRITE)
            isLocked = cache.tryWriteLockOnKey(lockKey, timeout);
        else
            throw new IllegalArgumentException("LockType not supported");

        return isLocked;
    }

    private synchronized void releaseLockInternal(Object lockKey, LockType lockType, boolean checkLockedByCurrentThread) {
        if(lockType == LockType.READ) {
            try {
                // isReadLockedByCurrentThread throws a "UnsupportedOperationException Querying of read lock is not supported" for standalone ehcache
                // so in that case, release the lock anyway
                if (!checkLockedByCurrentThread || checkLockedByCurrentThread && cache.isReadLockedByCurrentThread(lockKey))
                    cache.releaseReadLockOnKey(lockKey);
            } catch (UnsupportedOperationException uex){
                cache.releaseReadLockOnKey(lockKey);
            }
        } else if(lockType == LockType.WRITE) {
            if(!checkLockedByCurrentThread || checkLockedByCurrentThread && cache.isWriteLockedByCurrentThread(lockKey))
                cache.releaseWriteLockOnKey(lockKey);
        }
        else
            throw new IllegalArgumentException("LockType not supported");
    }

    protected EhcacheStreamValue getChunkValue(final Object cacheKey, int chunkIndex){
        EhcacheStreamValue chunkValue = null;
        Element chunkElem;
        if(null != (chunkElem = getChunkElement(cacheKey, chunkIndex)))
            chunkValue = (EhcacheStreamValue)chunkElem.getObjectValue();

        return chunkValue;
    }

    protected void putChunkValue(final Object cacheKey, int chunkIndex, byte[] chunkPayload){
        cache.put(new Element(new EhcacheStreamKey(cacheKey, chunkIndex), new EhcacheStreamValue(chunkPayload)));
    }

    private EhcacheStreamKey buildMasterKey(final Object cacheKey){
        return buildChunkKey(cacheKey, EhcacheStreamKey.MASTER_INDEX);
    }

    private EhcacheStreamKey buildChunkKey(final Object cacheKey, final int chunkIndex){
        return new EhcacheStreamKey(cacheKey, chunkIndex);
    }

    protected Element buildStreamMasterElement(final Object cacheKey, EhcacheStreamMaster ehcacheStreamMaster) {
        return new Element(buildMasterKey(cacheKey), ehcacheStreamMaster);
    }

    protected Element getStreamMasterElement(final Object cacheKey) {
        return cache.get(buildMasterKey(cacheKey));
    }

    private Element getChunkElement(final Object cacheKey, int chunkIndex) {
        return cache.get(buildChunkKey(cacheKey, chunkIndex));
    }

    protected EhcacheStreamMaster getStreamMasterFromCache(final Object cacheKey){
        EhcacheStreamMaster cacheMasterIndexValue = null;
        Element masterIndexElement = null;
        if(null != (masterIndexElement = getStreamMasterElement(cacheKey))) {
            cacheMasterIndexValue = (EhcacheStreamMaster)masterIndexElement.getObjectValue();
        }

        return cacheMasterIndexValue;
    }

    protected EhcacheStreamValue[] getStreamChunksFromCache(final Object cacheKey){
        return getStreamChunksFromStreamMaster(cacheKey, getStreamMasterFromCache(cacheKey));
    }

    protected EhcacheStreamValue[] getStreamChunksFromStreamMaster(final Object cacheKey, final EhcacheStreamMaster ehcacheStreamMaster){
        List chunkValues = null;
        if(null != ehcacheStreamMaster){
            chunkValues = new ArrayList(ehcacheStreamMaster.getChunkCounter());
            for(int i = 0; i < ehcacheStreamMaster.getChunkCounter(); i++){
                EhcacheStreamValue chunkValue = getChunkValue(cacheKey, i);
                if(null != chunkValue)
                    chunkValues.add(chunkValue);
            }
        }

        if(null == chunkValues)
            chunkValues = Collections.emptyList();

        return (EhcacheStreamValue[])chunkValues.toArray(new EhcacheStreamValue[chunkValues.size()]);
    }

    protected void clearChunksFromCacheKey(final Object cacheKey) {
        clearChunksFromStreamMaster(cacheKey, getStreamMasterFromCache(cacheKey));
    }

    protected void clearChunksFromStreamMaster(final Object cacheKey, final EhcacheStreamMaster ehcacheStreamMasterIndex) {
        if(null != ehcacheStreamMasterIndex){
            //remove all the chunk entries
            for(int i = 0; i < ehcacheStreamMasterIndex.getChunkCounter(); i++){
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
    protected boolean replaceIfEqualEhcacheStreamMaster(final Object cacheKey, EhcacheStreamMaster oldEhcacheStreamMaster, EhcacheStreamMaster newEhcacheStreamMaster) {
        boolean returnValue = false;
        if(null != oldEhcacheStreamMaster) {
            //replace old writeable element with new one using CAS operation for consistency
            returnValue = cache.replace(buildStreamMasterElement(cacheKey, oldEhcacheStreamMaster) , buildStreamMasterElement(cacheKey, newEhcacheStreamMaster));
        } else {
            Element previousElement = cache.putIfAbsent(buildStreamMasterElement(cacheKey, newEhcacheStreamMaster));
            returnValue = (previousElement == null);
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
    protected boolean replaceIfPresentEhcacheStreamMaster(final Object cacheKey, EhcacheStreamMaster newEhcacheStreamMaster) {
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
    protected boolean removeIfPresentEhcacheStreamMaster(final Object cacheKey, EhcacheStreamMaster oldEhcacheStreamMaster) {
        return cache.removeElement(buildStreamMasterElement(cacheKey, oldEhcacheStreamMaster));
    }
}

