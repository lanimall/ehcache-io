package org.ehcache.extensions.io.impl.writers;

import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Fabien Sanglier on 5/4/15.
 */
/*package protected*/ class EhcacheBufferedOutputStream extends EhcacheOutputStream {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheBufferedOutputStream.class);

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
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   bufferSize            the stream buffer size.
     * @param   ehcacheStreamWriter   the stream writer implementation
     *
     * @exception EhcacheStreamIllegalArgumentException if size &lt;= 0 or if ehcacheStreamWriter is null
     * @exception EhcacheStreamException if ehcacheStreamWriter could not be opened
     */
    public EhcacheBufferedOutputStream(int bufferSize, EhcacheStreamWriter ehcacheStreamWriter) throws EhcacheStreamException {
        super(ehcacheStreamWriter);

        if (bufferSize <= 0) {
            throw new EhcacheStreamIllegalArgumentException("Buffer size <= 0");
        }

        this.buf = new byte[bufferSize];
    }

    /**
     * Flush the internal buffer to cache
     * @throws IOException
     */
    private void flushBuffer() throws EhcacheStreamException {
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
    public void write(int b) throws EhcacheStreamException {
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
    public void write(byte[] b) throws EhcacheStreamException {
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
    public void write(byte[] b, int off, int len) throws EhcacheStreamException {
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
    public void flush() throws EhcacheStreamException {
        flushBuffer();
    }

    /**
     * Close the stream.
     * Will flush the stream and finalize the cache master index key here
     * @throws IOException
     */
    @Override
    public void close() throws EhcacheStreamException {
        try{
            flush();
        } finally {
            ehcacheStreamWriter.close();
        }
    }
}
