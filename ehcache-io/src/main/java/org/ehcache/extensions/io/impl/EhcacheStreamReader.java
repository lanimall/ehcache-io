package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;
import org.ehcache.extensions.io.EhcacheStreamException;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
/*package protected*/ class EhcacheStreamReader extends BaseEhcacheStream {

    protected volatile EhcacheStreamMaster currentStreamMaster;

    /*
     * The current position in the ehcache value chunk list.
     */
    protected volatile int cacheChunkIndexPos = 0;

    /*
     * The current offset in the ehcache value chunk
     */
    protected volatile int cacheChunkBytePos = 0;

    private volatile boolean isOpen = false;

    public EhcacheStreamReader(Cache cache, Object cacheKey) {
        super(cache, cacheKey);
    }

    //TODO: implement something better to return a better size
    //this is meant to be a general estimate without guarantees
    public int getSize() {
        EhcacheStreamMaster temp = ehcacheStreamUtils.getStreamMasterFromCache();
        return (null == temp)? 0: 1;
    }

    public void tryOpen(long timeout) throws EhcacheStreamException {
        if(!isOpen) {
            synchronized (this.getClass()) {
                if (!isOpen) {
                    try {
                        ehcacheStreamUtils.acquireReadOnMaster(timeout);
                    } catch (InterruptedException e) {
                        throw new EhcacheStreamException("Could not acquire the internal ehcache read lock", e);
                    }

                    this.currentStreamMaster = ehcacheStreamUtils.getStreamMasterFromCache();

                    isOpen = true;
                }
            }
        }

        if (!isOpen)
            throw new EhcacheStreamException("EhcacheStreamReader should be open at this point, something happened.");
    }

    public void close() throws EhcacheStreamException {
        if(isOpen) {
            synchronized (this.getClass()) {
                if (isOpen) {
                    ehcacheStreamUtils.releaseReadOnMaster();
                    isOpen = false;
                }
            }
        }

        if (isOpen)
            throw new EhcacheStreamException("EhcacheStreamReader should be closed at this point, something happened.");
    }

    public int read(byte[] outBuf, int bufferBytePos) throws EhcacheStreamException {
        if(!isOpen)
            throw new EhcacheStreamException("EhcacheStreamReader is not open...call open() first.");

        int byteCopied = 0;

        //then we get the index to know where we are in the writes
        if(null != currentStreamMaster && cacheChunkIndexPos < this.currentStreamMaster.getChunkCounter()){
            //get chunk from cache
            EhcacheStreamValue cacheChunkValue = ehcacheStreamUtils.getChunkValue(cacheChunkIndexPos);
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
                throw new EhcacheStreamException("Cache chunk [" + (cacheChunkIndexPos) + "] is null and should not be since we're within the cache total chunks [=" +  currentStreamMaster.getChunkCounter() + "] boundaries. Make sure the cache values are not evicted");
            }
        }

        return byteCopied;
    }
}
