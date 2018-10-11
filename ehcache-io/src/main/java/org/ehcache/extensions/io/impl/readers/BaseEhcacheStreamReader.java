package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.BaseEhcacheStream;
import org.ehcache.extensions.io.impl.model.EhcacheStreamChunk;

/**
 * Created by fabien.sanglier on 9/17/18.
 */
public abstract class BaseEhcacheStreamReader extends BaseEhcacheStream implements EhcacheStreamReader {
    /*
     * The current position in the ehcache value chunk list.
     */
    protected int cacheChunkIndexPos = 0;

    /*
     * The current offset in the ehcache value chunk
     */
    protected int cacheChunkBytePos = 0;


    protected BaseEhcacheStreamReader(Ehcache cache, Object cacheKey) {
        super(cache, cacheKey);
    }

    @Override
    public void close() throws EhcacheStreamException {
        this.cacheChunkIndexPos = 0;
        this.cacheChunkBytePos = 0;
    }

    protected int copyCacheChunksIntoBuffer(final byte[] outBuf, final int initialBufferBytePos, final int chunksTotalCount) {
        final int initialBufferAvailableSize = outBuf.length - initialBufferBytePos;
        int totalByteCopied = 0;
        int cacheChunkAvailableSize = 0;

        //repeat until currentixing exception buffer is full or we reach the max chunk index
        int currentByteCopied = 0;
        int currentBufferAvailableSize = 0;
        int currentBufferBytePos = 0;
        while (totalByteCopied < initialBufferAvailableSize && cacheChunkIndexPos < chunksTotalCount) {
            EhcacheStreamChunk cacheChunkValue = getEhcacheStreamUtils().getChunkValue(getPublicCacheKey(), cacheChunkIndexPos);

            //TODO: IMPORTANT!! checking for null cacheChunkValue is not enough
            //TODO: what if the cacheChunk was just being replaced by another write for example?
            //TODO: Currently, We would stream these bytes out even though these chunks are not ours...
            //TODO: We need some sort of concurrency check...maybe we add a checksum list in the stream master object
            //TODO: EG. as a write happens, it would store the checksums of each chunks in the stream master object,
            //TODO: And then, on read, we could reference and cross check each chunk from cache against expected checksum
            //TODO: i think that would be a good improvement for data consistency.
            if (null != cacheChunkValue && null != cacheChunkValue.getChunk()) {
                byte[] cacheChunk = cacheChunkValue.getChunk();

                //get the amount of bytes that could be copied from the current cache chunk
                cacheChunkAvailableSize = cacheChunk.length - cacheChunkBytePos;

                //re-adjust the buffer available and buffer position numbers taking into consideration the total number of bytes already written
                currentBufferAvailableSize = initialBufferAvailableSize - totalByteCopied;
                currentBufferBytePos = initialBufferBytePos + totalByteCopied;

                //calculate the number of bytes to copy from the cacheChunks into the destination buffer based on the buffer size that's available
                if (cacheChunkAvailableSize < currentBufferAvailableSize) {
                    currentByteCopied = cacheChunkAvailableSize;
                } else {
                    currentByteCopied = currentBufferAvailableSize;
                }

                System.arraycopy(cacheChunk, cacheChunkBytePos, outBuf, currentBufferBytePos, currentByteCopied);

                //track the chunk offset for next
                if (currentByteCopied < cacheChunkAvailableSize) {
                    cacheChunkBytePos = cacheChunkBytePos + currentByteCopied;
                } else { // it means we'll need to use the next chunk
                    cacheChunkIndexPos++;
                    cacheChunkBytePos = 0;
                }

                //make sure to add the current bytes copied to the total
                totalByteCopied += currentByteCopied;
            } else {
                cacheChunkAvailableSize = 0;

                //this should not happen within the cacheValueTotalChunks boundaries...hence exception
                throw new EhcacheStreamIllegalStateException(String.format("Cache chunk [=%s] is null and should not be " +
                        "since we're within the cache total chunks [=%s] boundaries." +
                        "Make sure the cache chunk values are not evicted (eg. pinning is not enabled?). " +
                        "Also, if cache is eventual, the entries may not all be synced yet..." +
                        "Consider changing your cache consistency to [strong]", cacheChunkIndexPos, chunksTotalCount));
            }
        }

        return totalByteCopied;
    }
}
