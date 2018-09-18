package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.BaseEhcacheStream;
import org.ehcache.extensions.io.impl.model.EhcacheStreamValue;

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

    protected int copyCacheChunksIntoBuffer(final byte[] outBuf, final int bufferBytePos, final int chunksTotalCount) throws EhcacheStreamException {
        int byteCopied = 0;

        // let's get the cache chunk based on the internal index tracker
        if(cacheChunkIndexPos < chunksTotalCount){
            EhcacheStreamValue cacheChunkValue = getEhcacheStreamUtils().getChunkValue(getCacheKey(), cacheChunkIndexPos);

            //TODO: IMPORTANT!! checking for null cacheChunkValue is not enough
            //TODO: what if the cacheChunk was just being replaced by another write for example?
            //TODO: Currently, We would stream these bytes out even though these chunks are not ours...
            //TODO: We need some sort of concurrency check...maybe we add a checksum list in the stream master object
            //TODO: EG. as a write happens, it would store the checksums of each chunks in the stream master object,
            //TODO: And then, on read, we could reference and cross check each chunk from cache against expected checksum
            //TODO: i think that would be a good improvement for data consistency.
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
                throw new EhcacheStreamIllegalStateException("Cache chunk [" + (cacheChunkIndexPos) + "] is null and should not be " +
                        "since we're within the cache total chunks [=" +  chunksTotalCount + "] boundaries." +
                        "Make sure the cache chunk values are not evicted (eg. pinning is not enabled?). " +
                        "Also, if cache is eventual, the entries may not all be synced yet..." +
                        "Consider changing your cache consistency to [strong]");
            }
        } else {
            //if we are here, we know we're done as we reached the end of the chunks...
            // BUT careful not to close here: the calling stream implementation may be calling this some more (eg. in case of buffer reading for example)
        }

        return byteCopied;
    }
}
