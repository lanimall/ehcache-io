package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.ExponentialWait;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.ehcache.extensions.io.impl.utils.WaitStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 7/24/18.
 */

/*
 * doc TBD
 */

/*package protected*/ class EhcacheStreamReaderNoLock extends BaseEhcacheStreamReader implements EhcacheStreamReader {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamReaderNoLock.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    /*
     * The current position in the ehcache value chunk list.
     */
    protected int cacheChunkIndexPos = 0;

    /*
     * The current offset in the ehcache value chunk
     */
    protected int cacheChunkBytePos = 0;

    //This is the copy of the master cache entry at time of open
    //we will use it to compare what we get during the successive gets
    private EhcacheStreamMaster activeStreamMaster;

    private volatile boolean isOpen = false;
    private final long openTimeoutMillis;

    public EhcacheStreamReaderNoLock(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
        super(cache, cacheKey);
        this.openTimeoutMillis = openTimeoutMillis;
    }

    //this is meant to be a general estimate without guarantees
    @Override
    public int getSize() {
        EhcacheStreamMaster temp = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
        return (null == temp)? 0: 1;
    }

    @Override
    public void tryOpen() throws EhcacheStreamException {
        if(openTimeoutMillis <= 0)
            throw new EhcacheStreamIllegalStateException(String.format("Open timeout [%d] may not be lower than 0", openTimeoutMillis));

        if (!isOpen) {
            try {
                activeStreamMaster = getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                        getCacheKey(),
                        openTimeoutMillis,
                        EhcacheStreamMaster.ComparatorType.NO_WRITER,
                        EhcacheStreamMaster.MutationField.READERS,
                        EhcacheStreamMaster.MutationType.NONE,   //here, on purpose, we don't want to increment anything...kind of a silent read so if there's a write, it will acquire its write
                        PropertyUtils.defaultReadsCasBackoffWaitStrategy
                );

                isOpen = true;
            } catch (Exception exc){
                isOpen = false;
                throw exc;
            }
        }

        if (!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader should be open at this point: something unexpected happened.");
    }

    @Override
    public void close() throws EhcacheStreamException {
        this.isOpen = false;
        this.activeStreamMaster = null;
        super.close();
    }

    @Override
    public int read(byte[] outBuf, int bufferBytePos) throws EhcacheStreamException {
        if(!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader is not open...call open() first.");

        int byteCopied = 0;

        // activeStreamMaster should not be null here since the open should have created it even if it was not there...
        // but let's check and log anyway just in case ... and returns nothing to copy
        if(null == activeStreamMaster) {
            if(logger.isWarnEnabled())
                logger.warn("activeStreamMaster should not be null here since the open should have created it even if it was not there...");

            return byteCopied;
        }

        // copy the cache chunks into the buffer based on the internal index tracker
        return copyCacheChunksIntoBuffer(outBuf, bufferBytePos, activeStreamMaster.getChunkCount());
    }
}
