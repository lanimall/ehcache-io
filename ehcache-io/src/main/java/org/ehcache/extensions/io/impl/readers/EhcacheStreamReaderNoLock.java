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
    EhcacheStreamMaster initialStreamMasterFromCache = null;

    private volatile boolean isOpen = false;
    private final long openTimeoutMillis;

    public EhcacheStreamReaderNoLock(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
        super(cache, cacheKey);
        this.openTimeoutMillis = openTimeoutMillis;
    }

    //TODO: implement something better to return a better size
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
                initialStreamMasterFromCache = getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
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
        this.initialStreamMasterFromCache = null;
        super.close();
    }

    @Override
    public int read(byte[] outBuf, int bufferBytePos) throws EhcacheStreamException {
        if(!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader is not open...call open() first.");

        int byteCopied = 0;

        //because we didn't increment the reader count, we need to check the cache to see if anything has changed since being open
        //And since we're not really atomic anyway (since we didn't lock anything in the open()), a simple get and compare would do i think...
        //overall, let's compare if the cache entry has not been written since we opened (the lastWritten bit would have changed)
        EhcacheStreamMaster currentStreamMaster = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
        boolean isWeaklyConsistent =
                currentStreamMaster != null &&
                        currentStreamMaster.getChunkCount() == initialStreamMasterFromCache.getChunkCount() &&
                        currentStreamMaster.getLastWrittenNanos() == initialStreamMasterFromCache.getLastWrittenNanos();

        if(!isWeaklyConsistent)
            throw new EhcacheStreamIllegalStateException("Concurrent modification exception: EhcacheStreamMaster has changed since opening...concurrent write must have happened.");

        // if cache entry is null, it's fine...means there's nothing to copy
        if(null == currentStreamMaster)
            return byteCopied;

        // copy the cache chunks into the buffer based on the internal index tracker
        return copyCacheChunksIntoBuffer(outBuf, bufferBytePos, currentStreamMaster.getChunkCount());
    }
}
