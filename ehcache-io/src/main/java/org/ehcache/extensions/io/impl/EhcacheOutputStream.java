package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by Fabien Sanglier on 5/4/15.
 */
public class EhcacheOutputStream extends OutputStream {

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
    protected final EhcacheStreamWriter ehcacheStreamWriter;
    protected final long ehcacheStreamWriterOpenTimeout;

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   cache           the underlying cache to copy data to
     * @param   cacheKey        the underlying cache key to read data from
     * @param   size            the buffer size.
     * @param   override        the mode in which to write the bytes (either override previous value, or append to previous value)
     * @param   openTimeout     the timeout for the stream writer open operation
     *
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public EhcacheOutputStream(Ehcache cache, Object cacheKey, int size, boolean override, long openTimeout) throws EhcacheStreamException {
        if (size <= 0) {
            throw new EhcacheStreamException(new IllegalArgumentException("Buffer size <= 0"));
        }
        this.buf = new byte[size];
        this.ehcacheStreamWriter = new EhcacheStreamWriter(cache, cacheKey, override);
        this.ehcacheStreamWriterOpenTimeout = openTimeout;
    }

    private void tryOpenInternalWriter() throws IOException{
        ehcacheStreamWriter.tryOpen(ehcacheStreamWriterOpenTimeout);
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
