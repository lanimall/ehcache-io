package org.ehcache.extensions.io.impl.writers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
import org.ehcache.extensions.io.impl.BaseEhcacheStream;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
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
    private static final boolean isDebug = logger.isDebugEnabled();

    private EhcacheStreamMaster activeStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;
    private volatile boolean isOpenMasterMutated = false;

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
                try {
                    //first, let's mark as write
                    activeStreamMaster = getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                            getCacheKey(),
                            openTimeoutMillis,
                            EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                            EhcacheStreamMaster.MutationField.WRITERS,
                            EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW,
                            PropertyUtils.defaultWritesCasBackoffWaitStrategy
                    );

                    isOpenMasterMutated = true;
                }  catch (EhcacheStreamTimeoutException te){
                    throw new EhcacheStreamTimeoutException("Could not open the stream within timeout",te);
                }

                //then once exclusive write, deal with override flag
                //if override set, let's clear the chunks for the master to keep things clean, and reset the chunk count on the local master instance
                if (override && activeStreamMaster.getChunkCount() > 0) {
                    if(isDebug)
                        logger.debug("Override requested: Clearing previous chunks...");

                    getEhcacheStreamUtils().clearChunksFromStreamMaster(getCacheKey(), activeStreamMaster);

                    //reset chunk count
                    activeStreamMaster.resetChunkCount();
                }

                isOpen = true;
            } catch (Exception exc){
                closeInternal();
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
            if(null != activeStreamMaster) {
                // finalize the EhcacheStreamMaster value with new chunk count by saving it in cache
                boolean replaced = getEhcacheStreamUtils().replaceIfPresentEhcacheStreamMaster(getCacheKey(), activeStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamIllegalStateException("Could not save the final ehcache stream index properly in cache...aborting");
            }
            closeInternal();
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
    }

    private void closeInternal() throws EhcacheStreamException {
        try {
            // reset the write state atomically so this entry can be written/read by others
            if (isOpenMasterMutated) {
                try {
                    getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                            getCacheKey(),
                            openTimeoutMillis,
                            EhcacheStreamMaster.ComparatorType.SINGLE_WRITER,
                            EhcacheStreamMaster.MutationField.WRITERS,
                            EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW,
                            PropertyUtils.defaultWritesCasBackoffWaitStrategy
                    );
                } catch (EhcacheStreamTimeoutException te) {
                    throw new EhcacheStreamTimeoutException("Could not close the stream within timeout", te);
                }
            }
        } finally {
            //clean the internal vars
            isOpen = false;
            isOpenMasterMutated = false;
            activeStreamMaster = null;
        }
    }

    /**
     * Writes data byte chunks to ehcache
     * buf: The internal buffer where data is stored.
     * count: The number of valid bytes in the buffer
     */
    public void writeData(byte[] buf, int count) throws EhcacheStreamException {
        if(!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter is not open...call open() first.");

        // activeStreamMaster should not be null here since the open should have created it even if it was not there...
        if(null == activeStreamMaster) {
            throw new EhcacheStreamIllegalStateException("activeStreamMaster should not be null at this point...");
        }

        //only 1 thread at a time should be able to reach this method...
        // because all other threads should be waiting in the tryOpen method still
        if(count > 0) {
            // let's add the chunk (overwrite anything in cache)
            getEhcacheStreamUtils().putChunkValue(getCacheKey(), activeStreamMaster.getAndIncrementChunkCount(), Arrays.copyOf(buf, count));
        }
    }
}
