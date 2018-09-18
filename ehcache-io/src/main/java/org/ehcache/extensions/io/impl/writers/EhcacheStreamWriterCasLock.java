package org.ehcache.extensions.io.impl.writers;

import net.sf.ehcache.Ehcache;
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

/*
 * This ehcache stream writer is in charge of saving the streamed bytes to backend ehcache store.
 * This ehcache stream writer must always be opened first (EhcacheStreamWriterNoLock.tryOpen()),
 * and successfully closed at the end (EhcacheStreamWriterNoLock.close()) for expected behavior.
 * The writer can only be created if no other writer or reader are currently actively working on this cache / cachekey combination.
 * Multiple threads trying to open this stream writer for the same cache / cachekey will be "synchronized" using ehcache CAS operation + loop.
 * Resulting in only 1 thread at a time being able to write to a single cache / cachekey...
 * while other threads will "wait" until either the cacheKey becomes available for writing, or the openTimeoutMillis is reached.
 */

/*package protected*/ class EhcacheStreamWriterCasLock extends BaseEhcacheStream implements EhcacheStreamWriter {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamWriterCasLock.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    private final boolean override;

    private volatile boolean isOpen = false;
    private final long openTimeoutMillis;

    public EhcacheStreamWriterCasLock(final Ehcache cache, final Object cacheKey, final boolean override, final long openTimeoutMillis) {
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
            try {
                //get the master index from cache, unless override is set
                EhcacheStreamMaster initialStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());

                EhcacheStreamMaster activeStreamMaster = getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                        getCacheKey(),
                        openTimeoutMillis,
                        override,
                        EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                        EhcacheStreamMaster.MutationField.WRITERS,
                        EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                );

                //if the new master has 0 chunk, it means it is an overwrite, so let's clear the chunks for the old master to keep things clean...
                if (activeStreamMaster.getChunkCount() == 0) {
                    logger.trace("Clearing previous chunks ...open");
                    getEhcacheStreamUtils().clearChunksFromStreamMaster(getCacheKey(), initialStreamMasterFromCache);
                }

                isOpen = true;
            } catch (Exception exc){
                isOpen = false;
                throw exc;
            }
        }

        if (!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be opened at this point or an exception should have been thrown...something unexpected happened.");
    }

    /**
     * Close() finalizes the stream master object and store it in cache.
     * This op must happen successfully otherwise this cache entry will remain un-writeable / un-readable forever until manual cleanup
     * If the current writer is not opened, close() will do nothing.
     * Calling close multiple times does not matter.
     */
    @Override
    public void close() throws EhcacheStreamException {
        if(isOpen) {
            // finalize the EhcacheStreamMaster value by saving it in cache with reader count decremented --
            // this op must happen otherwise this entry will remain un-writeable forever until manual cleanup
            getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                    getCacheKey(),
                    openTimeoutMillis,
                    false,
                    EhcacheStreamMaster.ComparatorType.SINGLE_WRITER,
                    EhcacheStreamMaster.MutationField.WRITERS,
                    EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
            );

            //clean the internal vars
            isOpen = false;
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
        if(!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter is not open...call open() first.");

        //only 1 thread at a time should be able to reach this method...
        // because all other threads should be waiting in the tryOpen method still
        if(count > 0) {
            //increment the chunk count
            EhcacheStreamMaster activeStreamMaster = getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                    getCacheKey(),
                    openTimeoutMillis,
                    false,
                    EhcacheStreamMaster.ComparatorType.SINGLE_WRITER,
                    EhcacheStreamMaster.MutationField.CHUNKS,
                    EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
            );

            // let's add the chunk (overwrite anything in cache)
            getEhcacheStreamUtils().putChunkValue(getCacheKey(), activeStreamMaster.getChunkCount() - 1, Arrays.copyOf(buf, count));
        }
    }
}
