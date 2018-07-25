package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;

import java.io.IOException;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
public class EhcacheStreamReader extends BaseEhcacheStream {

    /*
     * The current position in the ehcache value chunk list.
     */
    protected volatile int cacheChunkIndexPos = 0;

    /*
     * The current offset in the ehcache value chunk
     */
    protected volatile int cacheChunkBytePos = 0;


    public EhcacheStreamReader(Cache cache, Object cacheKey) {
        super(cache, cacheKey);
    }


    public void open() throws InterruptedException {
        synchronized (this.getClass()) {
            acquireExclusiveRead(LOCK_TIMEOUT);
        }
    }

    public void close() {
        synchronized (this.getClass()) {
            releaseExclusiveRead();
        }
    }

    public int read(byte[] outBuf, int bufferBytePos) throws InterruptedException, IOException {
        int byteCopied = 0;

        //first we try to acquire the read lock
        acquireExclusiveRead(LOCK_TIMEOUT);

        //then we get the index to know where we are in the writes
        EhcacheStreamMaster currentStreamMaster = getMasterIndexValue();
        if(null != currentStreamMaster && cacheChunkIndexPos < currentStreamMaster.getNumberOfChunk()){
            //get chunk from cache
            EhcacheStreamValue cacheChunkValue = getChunkValue(cacheChunkIndexPos);
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
                throw new IOException("Cache chunk [" + (cacheChunkIndexPos) + "] is null and should not be since we're within the cache total chunks [=" +  currentStreamMaster.getNumberOfChunk() + "] boundaries.");
            }
        }

        return byteCopied;
    }
}
