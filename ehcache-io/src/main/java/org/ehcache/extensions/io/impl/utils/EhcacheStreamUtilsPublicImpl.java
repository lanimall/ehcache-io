package org.ehcache.extensions.io.impl.utils;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    final PropertyUtils.ConcurrencyMode concurrencyMode = PropertyUtils.getEhcacheIOStreamsConcurrencyMode();

    EhcacheStreamUtilsPublicImpl(Ehcache cache) {
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

        switch (concurrencyMode){
            case WRITE_PRIORITY:
            case READ_COMMITTED_CASLOCKS:
                removed = ehcacheStreamUtilsInternal.removeEhcacheStream(
                        cacheKey,
                        timeout);
                break;
            case READ_COMMITTED_WITHLOCKS:
                removed = ehcacheStreamUtilsInternal.removeEhcacheStreamExplicitLocks(
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
     * @return      A list of all the public key objects
     */
    @Override
    public List getAllStreamEntryKeys(boolean checkForExpiry){
        return ehcacheStreamUtilsInternal.getAllStreamMasterPublicKeys(checkForExpiry);
    }

    /**
     * Get a list of all the public keys (the key objects used by the client apps) in cache
     *
     * @param       checkForExpiry        if true, returns only the keys that are not expired. NOTE: this could take much longer time if cache is large.
     * @param       includeNoReads        if true, includes only the keys that have no current read
     * @param       includeNoWrites        if true, includes only the keys that have no current write
     * @param       includeReadsOnly        if true, includes only the keys that have current reads
     * @param       includeWritesOnly        if true, includes only the keys that have current writes

     * @return      A list of public key objects
     */
    @Override
    public List getAllStreamEntryKeys(boolean checkForExpiry, boolean includeNoReads, boolean includeNoWrites, boolean includeReadsOnly, boolean includeWritesOnly){
        return ehcacheStreamUtilsInternal.getAllStreamMasterPublicKeys(checkForExpiry, includeNoReads, includeNoWrites, includeReadsOnly, includeWritesOnly);
    }

    /////////////////////////////////
    ////   End public accessors
    /////////////////////////////////
}

