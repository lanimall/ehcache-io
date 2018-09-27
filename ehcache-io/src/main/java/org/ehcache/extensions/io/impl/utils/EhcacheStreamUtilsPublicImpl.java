package org.ehcache.extensions.io.impl.utils;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamKey;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by fabien.sanglier on 8/2/18.
 */
public class EhcacheStreamUtilsPublicImpl implements IEhcacheStreamUtils {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamUtilsPublicImpl.class);

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
    public boolean removeStreamEntry(final Object cacheKey, long timeout) throws EhcacheStreamException {
        boolean removed = false;

        PropertyUtils.ConcurrencyMode concurrencyMode = PropertyUtils.getEhcacheIOStreamsConcurrencyMode();
        if(logger.isDebugEnabled())
            logger.debug("Creating a stream reader with Concurrency mode: {}", concurrencyMode.getPropValue());

        switch (concurrencyMode){
            case WRITE_PRIORITY:
            case READ_COMMITTED_CASLOCKS:
                removed = ehcacheStreamUtilsInternal.atomicRemoveEhcacheStreamMasterInCache(
                        cacheKey,
                        timeout);
                break;
            case READ_COMMITTED_WITHLOCKS:
                removed = ehcacheStreamUtilsInternal.atomicRemoveEhcacheStreamMasterInCacheExplicitLocks(
                        cacheKey,
                        timeout
                );
                break;
            default:
                throw new IllegalStateException("Not implemented");
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
                if(null != cacheKey
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

    @Override
    public List getAllStreamEntryKeysFilteredByState(boolean checkForExpiry, boolean includeCurrentWrites, boolean includeCurrentReads){
        List publicKeys;
        List internalKeys = (checkForExpiry)?ehcacheStreamUtilsInternal.getCache().getKeysWithExpiryCheck():ehcacheStreamUtilsInternal.getCache().getKeys();

        if(null != internalKeys && internalKeys.size() > 0) {
            publicKeys = new ArrayList(internalKeys.size());
            Iterator it = internalKeys.iterator();
            while( it.hasNext() ) {
                Object cacheKey = it.next();
                if(null != cacheKey
                        && cacheKey.getClass().equals(EhcacheStreamKey.class)
                        && ((EhcacheStreamKey)cacheKey).getChunkIndex() == EhcacheStreamKey.MASTER_INDEX){

                    EhcacheStreamMaster ehcacheStreamMaster = ehcacheStreamUtilsInternal.getStreamMasterFromCache(((EhcacheStreamKey)cacheKey).getCacheKey());
                    if(includeCurrentWrites && ehcacheStreamMaster.getWriters()>0) {
                        publicKeys.add(((EhcacheStreamKey) cacheKey).getCacheKey());
                    }

                    if(includeCurrentReads && ehcacheStreamMaster.getReaders()>0) {
                        publicKeys.add(((EhcacheStreamKey) cacheKey).getCacheKey());
                    }
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

