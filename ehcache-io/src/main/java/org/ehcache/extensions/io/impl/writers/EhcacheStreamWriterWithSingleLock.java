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
/*package protected*/ class EhcacheStreamWriterWithSingleLock extends BaseEhcacheStream implements EhcacheStreamWriter {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamWriterWithSingleLock.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    private EhcacheStreamMaster activeStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;
    private volatile boolean isOpenLockAcquired = false;
    private volatile boolean isOpenMasterMutated = false;

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
            if(isDebug)
                logger.debug("Trying to open a writer for key={}", (null != getPublicCacheKey())? getPublicCacheKey().toString():"null");

            try {
                try {
                    //always try to acquire the lock first
                    getEhcacheStreamUtils().acquireExclusiveWriteOnMaster(getPublicCacheKey(), openTimeoutMillis);

                    isOpenLockAcquired = true;

                    //Let's mark as write
                    activeStreamMaster = getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                            getPublicCacheKey(),
                            openTimeoutMillis,
                            EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                            EhcacheStreamMaster.MutationField.WRITERS,
                            EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW,
                            PropertyUtils.defaultWritesCasBackoffWaitStrategy
                    );

                    isOpenMasterMutated  = true;
                }  catch (EhcacheStreamTimeoutException te){
                    throw new EhcacheStreamTimeoutException("Could not open the stream within timeout",te);
                }

                //then once exclusive write, deal with override flag
                //if override set, let's clear the chunks for the master to keep things clean, and reset the chunk count on the local master instance
                if (override && activeStreamMaster.getChunkCount() > 0) {
                    if(isDebug)
                        logger.debug("Override requested: Clearing previous chunks...");

                    getEhcacheStreamUtils().clearChunksFromStreamMaster(getPublicCacheKey(), activeStreamMaster);

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
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be opened at this point: something unexpected happened.");
    }

    @Override
    public void close() throws EhcacheStreamException {
        if(isOpen) {
            if(null != activeStreamMaster) {
                // finalize the EhcacheStreamMaster value with new chunk count by saving it in cache
                boolean replaced = getEhcacheStreamUtils().replaceIfPresentEhcacheStreamMaster(getPublicCacheKey(), activeStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamIllegalStateException("Could not save the final ehcache stream index properly in cache...aborting");

                closeInternal();
            }
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
    }

    private void closeInternal() throws EhcacheStreamException {
        try {
            // finally, reset the write state atomically so this entry can be written/read by others
            if (isOpenMasterMutated) {
                try {
                    getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                            getPublicCacheKey(),
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
            try {
                if (isOpenLockAcquired) {
                    //release lock
                    getEhcacheStreamUtils().releaseExclusiveWriteOnMaster(getPublicCacheKey());
                }
            } finally {
                //clean the internal vars
                isOpen = false;
                isOpenLockAcquired =  false;
                isOpenMasterMutated = false;
                activeStreamMaster = null;
            }
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
            int chunkCount = activeStreamMaster.getChunkCount();

            // let's add the chunk (overwrite anything in cache)
            getEhcacheStreamUtils().putChunkValue(getPublicCacheKey(), chunkCount, Arrays.copyOf(buf, count));

            activeStreamMaster.getAndIncrementChunkCount();
//
//            try {
//                try {
//                    getEhcacheStreamUtils().tryLockInternal(
//                            getEhcacheStreamUtils().buildChunkKey(getPublicCacheKey(), chunkCount),
//                            EhcacheStreamUtilsInternal.LockType.WRITE,
//                            openTimeoutMillis
//                    );
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//
//                // let's add the chunk (overwrite anything in cache)
//                getEhcacheStreamUtils().putChunkValue(getPublicCacheKey(), chunkCount, Arrays.copyOf(buf, count));
//            } finally {
//                getEhcacheStreamUtils().releaseLockInternal(
//                        getEhcacheStreamUtils().buildChunkKey(getPublicCacheKey(), chunkCount),
//                        EhcacheStreamUtilsInternal.LockType.WRITE
//                );
//            }
//
//            activeStreamMaster.getAndIncrementChunkCount();
        }
    }
}
