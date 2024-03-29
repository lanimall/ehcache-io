package org.ehcache.extensions.io.impl.writers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.BaseEhcacheStream;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
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
            if(isDebug)
                logger.debug("Trying to open a writer for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

            //Let's mark as write
            activeStreamMaster = getEhcacheStreamUtils().openWriteOnMaster(
                    getPublicCacheKey(),
                    openTimeoutMillis
            );

            if(isDebug)
                logger.debug("Opened writer for key={} is {}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()), EhcacheStreamUtilsInternal.toStringSafe(activeStreamMaster));

            // activeStreamMaster cannot be null here since the open should have created it even if it was not there
            // and since nothing else can write to it while it's open
            if(activeStreamMaster == null || activeStreamMaster.getWriters() == 0)
                throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should not be null or have 0 writer at this point");

            // mark stream master as mutated -- important for the close operation...see comment in that section
            isOpenMasterMutated = true;

            //then once exclusive write, deal with override flag
            //if override set, let's clear the chunks for the master to keep things clean, and reset the chunk count on the local master instance
            if (override && activeStreamMaster.getChunkCount() > 0) {
                if(isDebug)
                    logger.debug("Override requested: Clearing previous chunks...");

                getEhcacheStreamUtils().clearChunksFromStreamMaster(getPublicCacheKey(), activeStreamMaster);

                //reset chunk count
                activeStreamMaster.resetChunkCount();
            }

            //mark as successfully open if we reach here
            isOpen = true;
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
        try {
            if (isOpen && null != activeStreamMaster) {
                // finalize the EhcacheStreamMaster value with new chunk count by saving it in cache
                boolean replaced = getEhcacheStreamUtils().replaceIfPresentEhcacheStreamMaster(getPublicCacheKey(), activeStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamIllegalStateException("Could not save the final ehcache stream index properly in cache...aborting");
            }
        } finally {
            closeInternal();
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
    }

    private void closeInternal() throws EhcacheStreamException {
        try {
            // reset the write state atomically so this entry can be written/read by others
            // it's important to check for this isOpenMasterMutated for the closing, as we only want to close if this current writer is the one that acquired the write
            // if we were closing the writer in every case (without checking if we're the one that modified it in the first place), then there would be a risk of closing the stream writer of another thread
            if(isOpenMasterMutated) {
                activeStreamMaster = getEhcacheStreamUtils().closeWriteOnMaster(
                        getPublicCacheKey(),
                        openTimeoutMillis
                );

                if(null != activeStreamMaster && activeStreamMaster.getWriters() > 0)
                    throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should not have more than 0 writer at this point");
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
            byte[] chunk = Arrays.copyOf(buf, count);
            int chunkIndex= activeStreamMaster.getChunkCount();

            getEhcacheStreamUtils().putChunkValue(getPublicCacheKey(), chunkIndex, chunk);

            //adding chunk descriptor to the master
            activeStreamMaster.addChunk(chunkIndex, chunk.length, EhcacheStreamUtilsInternal.createChunkCRC32(chunk));
        }
    }
}
