package org.ehcache.extensions.io.impl.writers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamConcurrentException;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.BaseEhcacheStream;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
/*package protected*/ class EhcacheStreamWriterWithSingleLock extends BaseEhcacheStream implements EhcacheStreamWriter {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamWriterWithSingleLock.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    private EhcacheStreamMaster initialOpenedStreamMaster;
    private EhcacheStreamMaster activeStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;
    private final long openTimeoutMillis;

    public EhcacheStreamWriterWithSingleLock(final Ehcache cache, final Object cacheKey, final boolean override, final long openTimeoutMillis) {
        super(cache, cacheKey);
        this.override = override;
        this.openTimeoutMillis = openTimeoutMillis;
    }

    /**
     * Open() creates a new stream master writer object and stores it in cache.
     */
    public void tryOpen() throws EhcacheStreamException {
        if(openTimeoutMillis <= 0)
            throw new EhcacheStreamIllegalStateException(String.format("Open timeout [%d] may not be lower than 0", openTimeoutMillis));

        if (!isOpen) {
            //always try to acquire the lock first
            getEhcacheStreamUtils().acquireExclusiveWriteOnMaster(getCacheKey(), openTimeoutMillis);

            //if we're here, we've successfully acquired the lock -- otherwise, a EhcacheStreamException would have been thrown
            //now, get the master index from cache, unless override is set
            EhcacheStreamMaster initialStreamMasterFromCache = null;
            try {
                initialStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());

                if(null != initialStreamMasterFromCache) {
                    if (!override) {
                        activeStreamMaster = new EhcacheStreamMaster(initialStreamMasterFromCache.getChunkCount());
                    } else {
                        activeStreamMaster = new EhcacheStreamMaster();
                    }
                } else {
                    activeStreamMaster = new EhcacheStreamMaster();
                }

                //set the master object with a new writer
                activeStreamMaster.addWriter();

                boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), initialStreamMasterFromCache, activeStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamConcurrentException("Concurrent write not allowed - Current cache entry with key[" + getCacheKey() + "] is currently being written...");

                //if the previous cas operation was successful, save (deep copy) the value from the cache into a instance variable
                EhcacheStreamMaster ehcacheStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
                activeStreamMaster = EhcacheStreamMaster.deepCopy(ehcacheStreamMasterFromCache);

                //save (deep copy) the newOpenStreamMaster for later usage (for CAS replace) when we close
                initialOpenedStreamMaster = EhcacheStreamMaster.deepCopy(activeStreamMaster);

                //if the new master has 0 chunk, it means it is an overwrite, so let's clear the chunks for the old master to keep things clean...
                if (activeStreamMaster.getChunkCount() == 0) {
                    if(isTrace)
                        logger.trace("Clearing previous chunks ...");
                    getEhcacheStreamUtils().clearChunksFromStreamMaster(getCacheKey(), initialStreamMasterFromCache);
                }

                //at this point, it's really open with consistency in cache
                isOpen = true;
            } catch (Exception exc){
                //release lock in case of issue opening
                getEhcacheStreamUtils().releaseExclusiveWriteOnMaster(getCacheKey());
                isOpen = false;
                throw exc;
            }
        }

        if (!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be opened at this point: something unexpected happened.");
    }

    @Override
    public void close() throws EhcacheStreamException {
        if(isOpen) {
            if (activeStreamMaster == null)
                throw new EhcacheStreamIllegalStateException("Trying to close, but activeStreamMaster is null, which should never happen here. Something unexpected happened.");

            // finalize the EhcacheStreamMaster value by saving it in cache with writer count decremented --
            // this op must happen otherwise this entry will remain un-writeable / un-readable forever until manual cleanup

            EhcacheStreamMaster backupWriterStreamMaster = EhcacheStreamMaster.deepCopy(activeStreamMaster);
            try {
                //decrement the writer count from the active stream master
                activeStreamMaster.removeWriter();

                boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), initialOpenedStreamMaster, activeStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamConcurrentException("CAS replace failed: Could not close the ehcache stream index properly.");

                //release the lock
                getEhcacheStreamUtils().releaseExclusiveWriteOnMaster(getCacheKey());

                //clean the internal vars
                isOpen = false;
                initialOpenedStreamMaster = null;
                activeStreamMaster = null;
            } catch (Exception exc) {
                //roll back the stream master for consistency (eg. if the client wants to call close again
                activeStreamMaster = backupWriterStreamMaster;
                throw exc;
            }
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
    }

    /**
     * Writes data byte chunks to ehcache
     * buf: The internal buffer where data is stored.
     * count: The number of valid bytes in the buffer
     */
    public void writeData(byte[] buf, int count) throws EhcacheStreamException {
        if(!isOpen || activeStreamMaster == null)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter is not open...call open() first.");

        //only 1 thread at a time should be able to reach this method...
        // because all other threads should be waiting in the tryOpen method still

        if(count > 0) {
            // let's add the chunk (overwrite anything in cache)
            getEhcacheStreamUtils().putChunkValue(getCacheKey(), activeStreamMaster.getChunkCount(), Arrays.copyOf(buf, count));

            //increment the chunk count
            activeStreamMaster.incrementChunkCount();
        }
    }
}
