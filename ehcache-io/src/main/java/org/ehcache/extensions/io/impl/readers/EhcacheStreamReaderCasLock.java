package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 7/24/18.
 */

/*
 * doc TBD
 */

/*package protected*/ class EhcacheStreamReaderCasLock extends BaseEhcacheStreamReader implements EhcacheStreamReader {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamReaderCasLock.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    //This is the copy of the master cache entry at time of open
    //we will use it to compare what we get during the successive gets
    private EhcacheStreamMaster activeStreamMaster;

    private volatile boolean isOpen = false;
    private volatile boolean isOpenMasterMutated = false;

    private final long openTimeoutMillis;

    public EhcacheStreamReaderCasLock(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
        super(cache, cacheKey);
        this.openTimeoutMillis = openTimeoutMillis;
    }

    //TODO: implement something better to return a better size
    //this is meant to be a general estimate without guarantees
    @Override
    public int getSize() {
        EhcacheStreamMaster temp = getEhcacheStreamUtils().getStreamMasterFromCache(getPublicCacheKey());
        return (null == temp)? 0: 1;
    }

    @Override
    public void tryOpen() throws EhcacheStreamException {
        if(openTimeoutMillis <= 0)
            throw new EhcacheStreamIllegalStateException(String.format("Open timeout [%d] may not be lower than 0", openTimeoutMillis));

        if (!isOpen) {
            if(isDebug)
                logger.debug("Trying to open a reader for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

            try {
                activeStreamMaster = getEhcacheStreamUtils().openReadOnMaster(
                        getPublicCacheKey(),
                        openTimeoutMillis
                );

                isOpenMasterMutated = true;
            } catch (EhcacheStreamTimeoutException te){
                throw new EhcacheStreamTimeoutException("Could not open the reader within timeout",te);
            }

            isOpen = true;
        }

        if (!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader should be open at this point: something unexpected happened.");
    }

    /**
     * Closes this reader and releases any system resources
     * associated with the reader.
     * Once the reader has been closed, further read() invocations will throw an EhcacheStreamException.
     * Closing a previously closed reader has no effect.
     *
     * @exception org.ehcache.extensions.io.EhcacheStreamException  if an I/O error occurs.
     */
    @Override
    public void close() throws EhcacheStreamException {
        if(isOpen) {
            closeInternal();
            super.close();
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
    }

    private void closeInternal() throws EhcacheStreamException {
        try {
            if (isOpenMasterMutated) {
                // finalize the EhcacheStreamMaster value by saving it in cache with reader count decremented --
                // this op must happen otherwise this entry will remain un-writeable forever until manual cleanup
                getEhcacheStreamUtils().closeReadOnMaster(
                        getPublicCacheKey(),
                        openTimeoutMillis
                );
            }
        } finally {
            //clean the internal vars
            isOpen = false;
            isOpenMasterMutated = false;
            activeStreamMaster = null;
        }
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
