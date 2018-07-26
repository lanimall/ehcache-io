package org.ehcache.extensions.io;

import net.sf.ehcache.Cache;
import org.ehcache.extensions.io.impl.EhcacheStreamWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by Fabien Sanglier on 5/4/15.
 */
public class EhcacheOutputStream extends OutputStream {
    private static int DEFAULT_BUFFER_SIZE = 5 * 1024 * 1024; // 5MB
    private static boolean OVERRIDE_DEFAULT = true;

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

    /**
     * The Internal Ehcache streaming access layer
     */
    protected EhcacheStreamWriter ehcacheStreamWriter;

    /**
     * Creates a new buffered output stream to write data to a cache
     *
     * @param   cache   the underlying cache to copy data to
     */
    public EhcacheOutputStream(Cache cache, Object cacheKey) {
        this(cache, cacheKey, OVERRIDE_DEFAULT);
    }

    /**
     * Creates a new buffered output stream to write data to a cache
     *
     * @param   cache   the underlying cache to copy data to
     */
    public EhcacheOutputStream(Cache cache, Object cacheKey, boolean override) {
        this(cache, cacheKey, override, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   cache   the underlying cache to copy data to
     * @param   bufferSize   the buffer size.
     * @exception IOException If the underlying openWrites operation is not successful
     */
    public EhcacheOutputStream(Cache cache, Object cacheKey, boolean override, int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        this.buf = new byte[bufferSize];
        this.ehcacheStreamWriter = new EhcacheStreamWriter(cache, cacheKey, override);
    }

    private void tryOpenInternalWriter() throws IOException{
        ehcacheStreamWriter.open();
    }

    private void closeInternalWriter() throws IOException {
        ehcacheStreamWriter.close();
    }

    /**
     * Flush the internal buffer to cache
     * @throws IOException
     */
    private void flushBuffer() throws IOException {
        //open the internal writer upon starting the writes
        tryOpenInternalWriter();

        if (count > 0) { // we're going to write here
            ehcacheStreamWriter.writeData(buf, count);

            count = 0; //reset buffer count
        }
    }

    /**
     *
     * @param b
     * @throws IOException
     */
    @Override
    public void write(int b) throws IOException {
        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte)b;
    }

    /**
     *
     * @param b
     * @throws IOException
     */
    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    /**
     *
     * @param b
     * @param off
     * @param len
     * @throws IOException
     */
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

    /**
     * Flush the stream
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        flushBuffer();
    }

    /**
     * Close the stream.
     * Will flush the stream and finalize the cache master index key here
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            //important to close this to release the locks etc...
            closeInternalWriter();
        }
    }
}
