package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
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
//    private EhcacheStreamMaster activeStreamMaster;

    private volatile boolean isOpen = false;
    private final long openTimeoutMillis;

    public EhcacheStreamReaderCasLock(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
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
                getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                        getCacheKey(),
                        openTimeoutMillis,
                        false,
                        EhcacheStreamMaster.ComparatorType.NO_WRITER,
                        EhcacheStreamMaster.MutationField.READERS,
                        EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
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
            // finalize the EhcacheStreamMaster value by saving it in cache with reader count decremented --
            // this op must happen otherwise this entry will remain un-writeable forever until manual cleanup
            getEhcacheStreamUtils().atomicMutateEhcacheStreamMasterInCache(
                    getCacheKey(),
                    openTimeoutMillis,
                    false,
                    EhcacheStreamMaster.ComparatorType.NO_WRITER,
                    EhcacheStreamMaster.MutationField.READERS,
                    EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
            );

            //clean the internal vars
            isOpen = false;
            super.close();
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
    }

    @Override
    public int read(byte[] outBuf, int bufferBytePos) throws EhcacheStreamException {
        if(!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader is not open...call open() first.");

        int byteCopied = 0;

        //get the stream master
        EhcacheStreamMaster activeStreamMaster = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());

        // if cache entry is null, it's fine...means there's nothing to copy
        if(null == activeStreamMaster)
            return byteCopied;

        // copy the cache chunks into the buffer based on the internal index tracker
        return copyCacheChunksIntoBuffer(outBuf, bufferBytePos, activeStreamMaster.getChunkCount());
    }
}
