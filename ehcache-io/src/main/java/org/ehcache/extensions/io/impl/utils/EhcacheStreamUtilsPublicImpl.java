package org.ehcache.extensions.io.impl.utils;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamKey;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.model.EhcacheStreamValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by fabien.sanglier on 8/2/18.
 */
public class EhcacheStreamUtilsPublicImpl implements IEhcacheStreamUtils {
    /*
     * The Internal Ehcache cache object
     */
    final EhcacheStreamUtilsInternal ehcacheStreamUtilsInternal;

    public EhcacheStreamUtilsPublicImpl(Ehcache cache) {
        //TODO: we should check if the cache is not null but maybe enforce "Pinning"?? (because otherwise cache chunks can disappear and that would mess up the data consistency...)
        this.ehcacheStreamUtilsInternal = new EhcacheStreamUtilsInternal(cache);
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
     * @exception   org.ehcache.extensions.io.EhcacheStreamException
     *
     */
    @Override
    public synchronized boolean removeStreamEntry(final Object cacheKey, long timeout) throws EhcacheStreamException {
        boolean removed = false;
        try {
            ehcacheStreamUtilsInternal.acquireExclusiveWriteOnMaster(cacheKey, timeout);

            //get stream master before removal
            EhcacheStreamMaster ehcacheStreamMaster = ehcacheStreamUtilsInternal.getStreamMasterFromCache(cacheKey);

            //remove stream master from cache (this op is the most important for consistency)
            removed = ehcacheStreamUtilsInternal.removeIfPresentEhcacheStreamMaster(cacheKey, ehcacheStreamMaster);

            // if success removal, clean up the chunks...
            // if that fails it's not good for space usage, but data will still be inconsistent.
            // and we'll catch this issue in the next verification steps...
            if(removed)
                ehcacheStreamUtilsInternal.clearChunksFromStreamMaster(cacheKey, ehcacheStreamMaster);

            //check that the master entry is actually removed
            if(null != ehcacheStreamUtilsInternal.getStreamMasterFromCache(cacheKey))
                throw new EhcacheStreamException("Master Entry was not removed as expected");

            //check that the other chunks are also removed
            EhcacheStreamValue[] chunkValues = ehcacheStreamUtilsInternal.getStreamChunksFromStreamMaster(cacheKey, ehcacheStreamMaster);
            if(null != chunkValues && chunkValues.length > 0)
                throw new EhcacheStreamException("Some chunk entries were not removed as expected");
        } finally {
            ehcacheStreamUtilsInternal.releaseExclusiveWriteOnMaster(cacheKey);
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
    @Override
    public boolean containsStreamEntry(final Object cacheKey) {
        return null != ehcacheStreamUtilsInternal.getStreamMasterFromCache(cacheKey);
    }

    /**
     * Get a list of all the public keys (the key objects used by the client apps) in cache
     *
     * @param       checkForExpiry        if true, returns only the keys that are not expired. NOTE: this could take much longer time if cache is large.
     * @return      A list of public key objects
     */
    @Override
    public List getAllStreamEntryKeys(boolean checkForExpiry){
        List publicKeys;
        List internalKeys = (checkForExpiry)?ehcacheStreamUtilsInternal.getCache().getKeysWithExpiryCheck():ehcacheStreamUtilsInternal.getCache().getKeys();

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
}

