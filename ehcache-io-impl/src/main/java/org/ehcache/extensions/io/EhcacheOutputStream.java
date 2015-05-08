package org.ehcache.extensions.io;

import net.sf.ehcache.Cache;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Created by Fabien Sanglier on 5/4/15.
 */
public class EhcacheOutputStream extends OutputStream {
    private static int DEFAULT_BUFFER_SIZE = 5 * 1024 * 1024; // 5MB

    /**
     * The internal buffer where data is stored.
     */
    protected byte buf[];

    /**
     * The number of valid bytes in the buffer. This value is always
     * in the range <tt>0</tt> through <tt>buf.length</tt>; elements
     * <tt>buf[0]</tt> through <tt>buf[count-1]</tt> contain valid
     * byte data.
     */
    protected int count;

    /*
     * The Internal Ehcache streaming access layer
     */
    protected final EhcacheStreamsDAL ehcacheStreamsDAL;

    /*
     * The number of cache entry chunks
     */
    protected volatile EhcacheStreamMasterIndex currentStreamMasterIndex = null;

    /**
     * Creates a new buffered output stream to write data to a cache
     *
     * @param   cache   the underlying cache to copy data to
     */
    public EhcacheOutputStream(Cache cache, Object cacheKey) {
        this(cache, cacheKey, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   cache   the underlying cache to copy data to
     * @param   size   the buffer size.
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public EhcacheOutputStream(Cache cache, Object cacheKey, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        this.buf = new byte[size];
        this.ehcacheStreamsDAL = new EhcacheStreamsDAL(cache,cacheKey);
        this.currentStreamMasterIndex = null;
    }

    public void clearCacheDataForKey() throws IOException {
        EhcacheStreamMasterIndex oldEhcacheStreamMasterIndex = ehcacheStreamsDAL.casReplaceEhcacheStreamMasterIndex(null, true);
        if(!ehcacheStreamsDAL.clearChunksForKey(oldEhcacheStreamMasterIndex)){
            // could not remove successfully all the chunk entries...
            // but that's not too terrible as long as the EhcacheStreamMasterIndex was removed properly (because the chunks will get overwritten on subsequent writes)
            // do nothing for now...
        }
    }

    /** Flush the internal buffer */
    private void flushBuffer() throws IOException {
        if (count > 0) { // we're going to write here
            //first time writing, so clear all cache entries for that key first (overwriting operation)
            //TODO: suspecting some sort of padlocking here for multi-thread protection...let's investigate later when basic functional is done
            if(null == currentStreamMasterIndex) {
                //set a new EhcacheStreamMasterIndex in write mode
                EhcacheStreamMasterIndex newStreamMasterIndex = new EhcacheStreamMasterIndex(EhcacheStreamMasterIndex.StreamOpStatus.CURRENT_WRITE);

                /*
                TODO let's think about this a bit more: if 2 thread arrive here, 1 should fail and the other should go through...fine.
                TODO But the one which failed is going to try to close the stream, which flush the buffer again...hence potential for overwritting the first one if it's already finished...
                */

                //set a new EhcacheStreamMasterIndex in write mode in cache if current element in cache is writable - else exception (protecting from concurrent writing)
                EhcacheStreamMasterIndex oldEhcacheStreamMasterIndex = ehcacheStreamsDAL.casReplaceEhcacheStreamMasterIndex(
                        new EhcacheStreamMasterIndex(EhcacheStreamMasterIndex.StreamOpStatus.CURRENT_WRITE),
                        true);

                //if previous cas operation successful, create a new EhcacheStreamMasterIndex for currentStreamMasterIndex (to avoid soft references issues to the cached value above)
                currentStreamMasterIndex = new EhcacheStreamMasterIndex(EhcacheStreamMasterIndex.StreamOpStatus.CURRENT_WRITE);

                //at this point, we're somewhat safe...entry is set to write-able
                //let's do some cleanup first
                ehcacheStreamsDAL.clearChunksForKey(oldEhcacheStreamMasterIndex);
            }

            ehcacheStreamsDAL.putChunkValue(currentStreamMasterIndex.getAndIncrementChunkIndex(), Arrays.copyOf(buf, count));
            count = 0; //reset buffer count
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte)b;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len >= buf.length) {
            //simple implementation...but works
            for(int i=0; i<len;i++){
                write(b[off+i]);
            }
            return;
        }
        if (len > buf.length - count) {
            flushBuffer();
        }
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
    }

    @Override
    public void close() throws IOException {
        flush();

        //finalize the EhcacheStreamMasterIndex value by saving it in cache
        if(null != currentStreamMasterIndex && currentStreamMasterIndex.isCurrentWrite()) {
            currentStreamMasterIndex.setAvailable();
            ehcacheStreamsDAL.casReplaceEhcacheStreamMasterIndex(currentStreamMasterIndex, false);
        }

        currentStreamMasterIndex = null;
    }
}
