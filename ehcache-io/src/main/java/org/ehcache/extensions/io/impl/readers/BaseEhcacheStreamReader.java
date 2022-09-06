package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.BaseEhcacheStream;
import org.ehcache.extensions.io.impl.model.EhcacheStreamChunk;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 9/17/18.
 */
public abstract class BaseEhcacheStreamReader extends BaseEhcacheStream implements EhcacheStreamReader {
    private static final Logger logger = LoggerFactory.getLogger(BaseEhcacheStreamReader.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    //This is the copy of the master cache entry at time of open
    //we will use it to compare what we get during the successive gets
    EhcacheStreamMaster activeStreamMaster = null;

    private final Object openLock = new Object();
    volatile boolean isOpen = false;
    volatile boolean isClosing = false;

    /*
     * The current position in the ehcache value chunk list.
     */
    int cacheChunkIndexPos = 0;

    /*
     * The current offset in the ehcache value chunk
     */
    int cacheChunkBytePos = 0;

    /**
     * The internal buffer array where the chunk data is temporarily stored...especially useful for byte-by-byte read()
     */
    protected volatile byte[] tempChunkData;
    protected volatile boolean markRefillTempChunkData = false;

    protected BaseEhcacheStreamReader(Ehcache cache, Object cacheKey) {
        super(cache, cacheKey);
    }

    //get chunk data from temp store, or refill it with more data if marked as such
    private byte[] getChunkData() throws EhcacheStreamException {
        if(null == tempChunkData || markRefillTempChunkData){
            EhcacheStreamChunk cacheChunkValue = getEhcacheStreamUtils().getChunkValue(getPublicCacheKey(), cacheChunkIndexPos);

            //TODO: IMPORTANT!! checking for null cacheChunkValue is not enough
            //TODO: what if the cacheChunk was just being replaced by another write for example?
            //TODO: Currently, We would stream these bytes out even though these chunks are not ours...
            //TODO: We need some sort of concurrency check...maybe we add a checksum list in the stream master object
            //TODO: EG. as a write happens, it would store the checksums of each chunks in the stream master object,
            //TODO: And then, on read, we could reference and cross check each chunk from cache against expected checksum
            //TODO: i think that would be a good improvement for data consistency.
            if (null != cacheChunkValue && null != cacheChunkValue.getChunk()) {
                tempChunkData = new byte[cacheChunkValue.getChunk().length];
                System.arraycopy(cacheChunkValue.getChunk(), 0, tempChunkData, 0, cacheChunkValue.getChunk().length);
            } else {
                //clear it
                tempChunkData = null;
            }

            if(isDebug)
                logger.debug("Just fetched chunk from cache with length = {}", (null != tempChunkData)?tempChunkData.length:0);
        }


        return tempChunkData;
    }

    //open the reader
    public void open() throws EhcacheStreamException {
        if (!isOpen) {
            synchronized (openLock) {
                if (!isOpen) {
                    if(isDebug)
                        logger.debug("Trying to open a reader for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

                    //perform one-time inits
                    oneTimeInit();

                    //mark as successfully open if we reach here
                    isOpen = true;

                    if(isDebug && isOpen)
                        logger.debug("Success reader open for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));
                }
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
        if(isDebug)
            logger.debug("Closing reader for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

        //multi-thread barrier here ot make sure 1 thread only closes
        synchronized (openLock) {
            if (isClosing) {
                return;
            }
            isClosing = true;
        }

        if(isDebug)
            logger.debug("About to clean up reader instance data for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

        try {
            oneTimeCleanup();
        } finally {
            this.cacheChunkIndexPos = 0;
            this.cacheChunkBytePos = 0;
            this.activeStreamMaster = null;
            this.tempChunkData = null;
            this.markRefillTempChunkData = false;
            this.isOpen = false;
            this.isClosing = false;
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
    }

    abstract void oneTimeInit() throws EhcacheStreamException;

    abstract void oneTimeCleanup() throws EhcacheStreamException;

    void checkIfOpen() {
        if(!isOpen()) throw new EhcacheStreamIllegalStateException("EhcacheStreamReader is not open...call open() first.");
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Attempt to return the number of bytes still to be read in the stream
     * Approximation: Use the current temp chunk size to approximate the total remaining size in the following chunks
     * Will return 0 if stream is
     */
    @Override
    public int available() throws EhcacheStreamException {
        int bytesAvailable;
        checkIfOpen();

        // check if activeStreamMaster or the chunk index is above max chunk count - in both case, return end of stream
        final int chunksTotalCount;
        if(null == activeStreamMaster || cacheChunkIndexPos >= (chunksTotalCount = activeStreamMaster.getChunkCount())) {
            bytesAvailable = 0;
        } else {
            if (null == getChunkData()) {
                bytesAvailable = 0;
            } else {
                //calculate the remaining number of bytes in this chunk
                bytesAvailable = getChunkData().length - cacheChunkBytePos;

                //  and add all the remaining chunks to read,
                long[] chunkSizeInBytes = activeStreamMaster.getAllChunkSizeInBytes();
                int startIndex = cacheChunkIndexPos + 1; // +1 to account for the current bytes calculated above
                for (int i = startIndex; i < chunksTotalCount; i++) {
                    bytesAvailable += chunkSizeInBytes[i];
                }
            }
        }

        if(isDebug) {
            long totalSize = (null != activeStreamMaster)?activeStreamMaster.getChunksTotalSizeInBytes():0L;
            logger.debug("Available bytes: {} out of {}", bytesAvailable, totalSize);
        }

        return bytesAvailable;
    }

    @Override
    public int read() throws EhcacheStreamException {
        int byteRead;
        checkIfOpen();

        // check if activeStreamMaster or the chunk index is above max chunk count - in both case, return end of stream
        final int chunksTotalCount;
        if(null == activeStreamMaster || cacheChunkIndexPos >= (chunksTotalCount = activeStreamMaster.getChunkCount())) {
            byteRead = -1;
        } else {
            byte[] cacheChunk = getChunkData();
            if (null != cacheChunk) {
                //get the byte at position
                byteRead = cacheChunk[cacheChunkBytePos] & 0xff;

                //track the chunk offset for next
                int cacheChunkAvailableSize = cacheChunk.length - cacheChunkBytePos;
                if (cacheChunkAvailableSize > 1) {
                    cacheChunkBytePos++;
                    markRefillTempChunkData = false;
                } else { // it means we'll need to use the next chunk
                    cacheChunkIndexPos++;
                    cacheChunkBytePos = 0;
                    markRefillTempChunkData = true;
                }
            } else {
                //this should not happen within the cacheValueTotalChunks boundaries...hence exception
                throw new EhcacheStreamIllegalStateException(String.format("Cache chunk [=%s] is null and should not be " +
                        "since we're within the cache total chunks [=%s] boundaries." +
                        "Make sure the cache chunk values are not evicted (eg. pinning is not enabled?). " +
                        "Also, if cache is eventual, the entries may not all be synced yet..." +
                        "Consider changing your cache consistency to [strong]", cacheChunkIndexPos, chunksTotalCount));
            }
        }

        if(isDebug) {
            logger.debug("Read {} bytes", byteRead);
        }

        return byteRead;
    }

    /**
     * Reads up to <code>len</code> bytes of data from the input stream into
     * an array of bytes.  An attempt is made to read as many as
     * <code>len</code> bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     *
     * <p> This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     *
     * <p> If <code>len</code> is zero, then no bytes are read and
     * <code>0</code> is returned; otherwise, there is an attempt to read at
     * least one byte. If no byte is available because the stream is at end of
     * file, the value <code>-1</code> is returned; otherwise, at least one
     * byte is read and stored into <code>b</code>.
     * @param      outBuf     the buffer into which the data is read.
     * @param      offset   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param      len   the maximum number of bytes to read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the stream has been reached.
     */
    int read1(final byte[] outBuf, final int offset, final int len) throws EhcacheStreamException {
        int totalByteCopied;

        if (outBuf == null) {
            throw new NullPointerException();
        } else if ((offset | len | (offset + len) | (outBuf.length - (offset + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        // check if activeStreamMaster or the chunk index is above max chunk count - in both case, return end of stream
        final int chunksTotalCount;
        if (len == 0) {
            totalByteCopied = 0;
        } else if(null == activeStreamMaster || cacheChunkIndexPos >= (chunksTotalCount = activeStreamMaster.getChunkCount())) {
            totalByteCopied = -1;
        } else {
            //calculate the max bytes that can be copied based on buffer size, offset, and length request
            final int maxBytesToCopy = Math.min(outBuf.length - offset, len);

            //repeat until all the bytes are copied in the buffer (either len is reached, or buffer is full) OR we reached the max chunk index in cache
            totalByteCopied = 0;
            while (totalByteCopied < maxBytesToCopy && cacheChunkIndexPos < chunksTotalCount) {
                byte[] cacheChunk = getChunkData();
                if (null != cacheChunk) {
                    //get the amount of bytes that could be copied from the current cache chunk
                    int cacheChunkAvailableSize = cacheChunk.length - cacheChunkBytePos;

                    //re-adjust the buffer available and buffer position numbers taking into consideration the total number of bytes already written
                    int currentBufferAvailableSize = maxBytesToCopy - totalByteCopied;
                    int currentBufferBytePos = offset + totalByteCopied;

                    //calculate the number of bytes to copy from the cacheChunks into the destination buffer based on the buffer size that's available
                    int currentByteCopied;
                    if (cacheChunkAvailableSize < currentBufferAvailableSize) {
                        currentByteCopied = cacheChunkAvailableSize;
                    } else {
                        currentByteCopied = currentBufferAvailableSize;
                    }

                    System.arraycopy(cacheChunk, cacheChunkBytePos, outBuf, currentBufferBytePos, currentByteCopied);

                    //track the chunk offset for next
                    if (currentByteCopied < cacheChunkAvailableSize) {
                        cacheChunkBytePos += currentByteCopied;
                        markRefillTempChunkData = false;
                    } else { // it means we'll need to use the next chunk
                        cacheChunkIndexPos++;
                        cacheChunkBytePos = 0;
                        markRefillTempChunkData = true;
                    }

                    //make sure to add the current bytes copied to the total
                    totalByteCopied += currentByteCopied;
                } else {
                    //this should not happen within the cacheValueTotalChunks boundaries...hence exception
                    throw new EhcacheStreamIllegalStateException(String.format("Cache chunk [=%s] is null and should not be " +
                            "since we're within the cache total chunks [=%s] boundaries." +
                            "Make sure the cache chunk values are not evicted (eg. pinning is not enabled?). " +
                            "Also, if cache is eventual, the entries may not all be synced yet..." +
                            "Consider changing your cache consistency to [strong]", cacheChunkIndexPos, chunksTotalCount));
                }
            }
        }

        if(isDebug) {
            logger.debug("Read {} bytes", totalByteCopied);
        }

        return totalByteCopied;
    }


}
