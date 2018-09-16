package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.BaseEhcacheStream;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.model.EhcacheStreamValue;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
/*package protected*/ class EhcacheStreamReaderWithSingleLock extends BaseEhcacheStream implements EhcacheStreamReader {

    protected EhcacheStreamMaster currentStreamMaster;

    /*
     * The current position in the ehcache value chunk list.
     */
    protected int cacheChunkIndexPos = 0;

    /*
     * The current offset in the ehcache value chunk
     */
    protected int cacheChunkBytePos = 0;

    private volatile boolean isOpen = false;
    private final long openTimeoutMillis;

    public EhcacheStreamReaderWithSingleLock(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
        super(cache, cacheKey);
        this.openTimeoutMillis = openTimeoutMillis;
    }

    //TODO: implement something better to return a better size
    //this is meant to be a general estimate without guarantees
    public int getSize() {
        EhcacheStreamMaster temp = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
        return (null == temp)? 0: 1;
    }

    public void tryOpen() throws EhcacheStreamException {
        if(openTimeoutMillis <= 0)
            throw new EhcacheStreamIllegalStateException(String.format("Open timeout [%d] may not be lower than 0", openTimeoutMillis));

        if (!isOpen) {
            getEhcacheStreamUtils().acquireReadOnMaster(getCacheKey(), openTimeoutMillis);

            try {
                EhcacheStreamMaster ehcacheStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
                this.currentStreamMaster = EhcacheStreamMaster.deepCopy(ehcacheStreamMasterFromCache);

                isOpen = true;
            } catch (Exception exc){
                //release lock
                getEhcacheStreamUtils().releaseReadOnMaster(getCacheKey());
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
    public void close() throws EhcacheStreamException {
        if (isOpen) {
            getEhcacheStreamUtils().releaseReadOnMaster(getCacheKey());
            cacheChunkIndexPos = 0;
            cacheChunkBytePos = 0;
            currentStreamMaster = null;
            isOpen = false;
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader should be closed at this point: something unexpected happened.");
    }

    public int read(byte[] outBuf, int bufferBytePos) throws EhcacheStreamException {
        if(!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader is not open...call open() first.");

        int byteCopied = 0;

        //then we get the index to know where we are in the writes
        if(null != currentStreamMaster && cacheChunkIndexPos < this.currentStreamMaster.getChunkCount()){
            //get chunk from cache
            EhcacheStreamValue cacheChunkValue = getEhcacheStreamUtils().getChunkValue(getCacheKey(), cacheChunkIndexPos);
            if(null != cacheChunkValue && null != cacheChunkValue.getChunk()) {
                byte[] cacheChunk = cacheChunkValue.getChunk();

                //calculate the number of bytes to copy from the cacheChunks into the destination buffer based on the buffer size that's available
                if(cacheChunk.length - cacheChunkBytePos < outBuf.length - bufferBytePos){
                    byteCopied = cacheChunk.length - cacheChunkBytePos;
                } else {
                    byteCopied = outBuf.length - bufferBytePos;
                }

                System.arraycopy(cacheChunk, cacheChunkBytePos, outBuf, bufferBytePos, byteCopied);

                //track the chunk offset for next
                if(byteCopied < cacheChunk.length - cacheChunkBytePos) {
                    cacheChunkBytePos = cacheChunkBytePos + byteCopied;
                } else { // it means we'll need to use the next chunk
                    cacheChunkIndexPos++;
                    cacheChunkBytePos = 0;
                }
            } else {
                //this should not happen within the cacheValueTotalChunks boundaries...hence exception
                throw new EhcacheStreamIllegalStateException("Cache chunk [" + (cacheChunkIndexPos) + "] is null " +
                        "and should not be since we're within the cache total chunks [=" +  currentStreamMaster.getChunkCount() + "] boundaries.");
            }
        } else {
            //if we are here, we know we're done as we reached the end of the chunks...
            // BUT careful not to close here: the calling stream implementation may be calling this some more (eg. in case of buffer reading for example)
        }

        return byteCopied;
    }
}
