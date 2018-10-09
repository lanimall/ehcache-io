package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 7/24/18.
 */

/*
 * This implementation will wait initially if a write is currently being done,
 * BUT it will not broadcast a READ state so a WRITE will be able to be acquired during this READ.
 * The benefit is that a write will never wait on this read (write  priority) and there's no need to "unlock" at the end
 * The drawback is that an error may be thrown upon discovery that a write is going on...
 * And to discover if read is inconsistent, I have to fetch the master entry on each read to do a simple consistency check based on last written nano time
 * which does not seem to be a performance impact due to local caching
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
                activeStreamMaster = getEhcacheStreamUtils().openSilentReadOnMaster(
                        getPublicCacheKey(),
                        openTimeoutMillis
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

        // activeStreamMaster could be null here if there was no entry in cache in the first place...
        if(null == activeStreamMaster) {
            return byteCopied;
        }

        //because we didn't increment the reader count, we need to check the cache to see if anything has changed since being open
        //And since we're not really atomic anyway (since we didn't lock anything in the open()), a simple get and compare would do i think...
        //overall, let's compare if the cache entry has not been written since we opened (the lastWritten bit would have changed)
        EhcacheStreamMaster currentStreamMaster = getEhcacheStreamUtils().getStreamMasterFromCache(getPublicCacheKey());
        boolean isWeaklyConsistent =
                currentStreamMaster != null &&
                        currentStreamMaster.getChunkCount() == activeStreamMaster.getChunkCount() &&
                        currentStreamMaster.getLastWrittenTime() == activeStreamMaster.getLastWrittenTime();

        if(!isWeaklyConsistent)
            throw new EhcacheStreamIllegalStateException("Concurrent modification exception: EhcacheStreamMaster has changed since opening: a concurrent write must have happened. Consider retrying in a bit.");

        try {
            // copy the cache chunks into the buffer based on the internal index tracker
            return copyCacheChunksIntoBuffer(outBuf, bufferBytePos, activeStreamMaster.getChunkCount());
        } catch (EhcacheStreamIllegalStateException exc){
            throw new EhcacheStreamIllegalStateException(String.format("Could not read the cache chunk. Current StreamMaster[=%s]", EhcacheStreamUtilsInternal.toStringSafe(activeStreamMaster)), exc);
        }
    }
}
