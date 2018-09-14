package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamConcurrentException;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;

import java.io.Closeable;

/**
 * Created by fabien.sanglier on 7/24/18.
 */

/*
 * doc TBD
 */

/*package protected*/ class EhcacheStreamReaderNoLock extends BaseEhcacheStream implements Closeable {

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
    protected EhcacheStreamMaster initialOpenedStreamMaster;

    private volatile boolean isOpen = false;

    public EhcacheStreamReaderNoLock(Ehcache cache, Object cacheKey) {
        super(cache, cacheKey);
    }

    //TODO: implement something better to return a better size
    //this is meant to be a general estimate without guarantees
    public int getSize() {
        EhcacheStreamMaster temp = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
        return (null == temp)? 0: 1;
    }

    public void tryOpen(long timeoutMillis) throws EhcacheStreamException {
        if(timeoutMillis <= 0)
            throw new EhcacheStreamException(String.format("Open timeout [%d] may not be lower than 0", timeoutMillis));

        EhcacheStreamMaster test = new EhcacheStreamMaster(null);
        if (!isOpen) {
            try {
                //TODO: sync protect that loop?
                // get the stream master for reading
                // if it's null, get out right away
                // if it's not null, check for available flag. if flag is not available for current reading, try a couple of time with some wait
                EhcacheStreamMaster ehcacheStreamMasterFromCache = null;
                long t1 = System.currentTimeMillis();
                long t2 = t1; //this ensures that the while always happen at least once!
                while (t2 - t1 <= timeoutMillis) {
                    ehcacheStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
                    if(null == ehcacheStreamMasterFromCache || null != ehcacheStreamMasterFromCache && !ehcacheStreamMasterFromCache.isCurrentWrite())
                        break;

                    try {
                        Thread.sleep(50L);
                    } catch (InterruptedException e) {
                        throw new EhcacheStreamException("Thread sleep interrupted", e);
                    } finally {
                        Thread.yield();
                    }
                    t2 = System.currentTimeMillis();
                }

                //if the stream master is still being written at the end of the tries, stop trying...
                if (null != ehcacheStreamMasterFromCache && ehcacheStreamMasterFromCache.isCurrentWrite()) {
                    throw new EhcacheStreamException(String.format("Could not acquire a read within timeout %d ms.", t2 - t1));
                }

                //once the stream master is available for reading, save it
                this.initialOpenedStreamMaster = (null != ehcacheStreamMasterFromCache)?EhcacheStreamMaster.deepCopy(ehcacheStreamMasterFromCache):null;

                isOpen = true;
            } catch (Exception exc){
                isOpen = false;
                throw new EhcacheStreamException(exc);
            }
        }

        if (!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader should be open at this point: something unexpected happened.");
    }

    @Override
    public void close() throws EhcacheStreamException {
        this.isOpen = false;
        this.cacheChunkIndexPos = 0;
        this.cacheChunkBytePos = 0;
        this.initialOpenedStreamMaster = null;
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    public int read(byte[] outBuf, int bufferBytePos) throws EhcacheStreamException {
        if(!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamReader is not open...call open() first.");

        int byteCopied = 0;

        //get the stream master from cache
        EhcacheStreamMaster currentStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());

        //check if the stream master we just got is the same as the one we opened with
        boolean isConsistent = EhcacheStreamMaster.compare(currentStreamMasterFromCache, initialOpenedStreamMaster);
        if(!isConsistent)
            throw new EhcacheStreamConcurrentException("Concurrent modification exception: EhcacheStreamMaster has changed since opening...concurrent write must have happened."); //TODO: maybe a new EhcacheStreamConsistencyException?

        // now we know we are good with consistent EhcacheStreamMaster from cache, let's do the work.

        // if cache entry is null, it's fine...means there's nothing to copy
        if(null == currentStreamMasterFromCache)
            return byteCopied;

        // if currentStreamMasterFromCache is something consistent, let's get the chunk
        // we can get the index to know where we are in the writes
        if(cacheChunkIndexPos < currentStreamMasterFromCache.getChunkCount()){
            //get chunk from cache
            //And in case it's eventual consistency, let's try multiple times...
            EhcacheStreamValue cacheChunkValue = null;
            int maxTries = 10;
            int tryCount = 0;
            while(tryCount < maxTries && null == cacheChunkValue){
                cacheChunkValue = getEhcacheStreamUtils().getChunkValue(getCacheKey(), cacheChunkIndexPos);
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
                    try {
                        Thread.sleep(tryCount * 1L); //sliding wait time
                    } catch (InterruptedException e) {
                        throw new EhcacheStreamException("Thread sleep interrupted", e);
                    } finally {
                        Thread.yield();
                    }
                }
                tryCount++;
            }

            // if the cache chunk is still null, this should not really happen within the cacheValueTotalChunks boundaries
            // there is potential for this to happen if cache is eventual, but we supposedly waited and retried to get it...
            // Overall let's throw an exception to let the client know...
            if(null == cacheChunkValue) {
                throw new EhcacheStreamIllegalStateException("Cache chunk [" + (cacheChunkIndexPos) + "] is null and should not be " +
                        "since we're within the cache total chunks [=" +  currentStreamMasterFromCache.getChunkCount() + "] boundaries." +
                        "Make sure the cache chunk values are not evicted (eg. pinning is not enabled?). " +
                        "Also, if cache is eventual, consider increasing the internal chunk retrieval retries.");
            }
        }

        return byteCopied;
    }
}
