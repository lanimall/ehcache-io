package org.ehcache.extensions.io.impl.writers;

import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by Fabien Sanglier on 5/4/15.
 */
/*package protected*/ class EhcacheOutputStream extends OutputStream {

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

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   bufferSize            the stream buffer size.
     * @param   ehcacheStreamWriter   the stream writer implementation
     *
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public EhcacheOutputStream(int bufferSize, EhcacheStreamWriter ehcacheStreamWriter) {
        if (bufferSize <= 0) {
            throw new EhcacheStreamIllegalArgumentException("Buffer size <= 0");
        }

        this.buf = new byte[bufferSize];
        this.ehcacheStreamWriter = ehcacheStreamWriter;
    }

    //try open the writer (if writer already opened, will not re-open again
    //Important: don't put this in the constructor to make sure the object gets always constructed
    private void tryOpenEhcacheStreamWriter() throws EhcacheStreamException {
        //open the underlying writer
        if(null != ehcacheStreamWriter)
            ehcacheStreamWriter.tryOpen();
    }

    private void tryCloseEhcacheStreamWriter() throws EhcacheStreamException {
        if(null != ehcacheStreamWriter)
            ehcacheStreamWriter.close();
    }

    /**
     * Flush the internal buffer to cache
     * @throws IOException
     */
    private void flushBuffer() throws IOException {
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
        tryOpenEhcacheStreamWriter();
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
        tryOpenEhcacheStreamWriter();
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
        tryOpenEhcacheStreamWriter();
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
        try{
            flush();
        } finally {
            tryCloseEhcacheStreamWriter();
        }
    }
}
