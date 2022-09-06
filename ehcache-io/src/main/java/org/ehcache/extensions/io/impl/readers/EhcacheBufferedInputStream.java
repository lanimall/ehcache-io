package org.ehcache.extensions.io.impl.readers;

import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Created by Fabien Sanglier on 5/4/15.
 */
/*package protected*/ class EhcacheBufferedInputStream extends EhcacheInputStream {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheBufferedInputStream.class);

    /**
     * The internal buffer array where the data is stored.
     */
    protected volatile byte buf[];

    /**
     * Atomic updater to provide compareAndSet for buf. This is
     * necessary because closes can be asynchronous. We use nullness
     * of buf[] as primary indicator that this stream is closed. (The
     * "in" field is also nulled out on close.)
     */
    private static final
    AtomicReferenceFieldUpdater<EhcacheBufferedInputStream, byte[]> bufUpdater =
            AtomicReferenceFieldUpdater.newUpdater
                    (EhcacheBufferedInputStream.class,  byte[].class, "buf");

    /**
     * The index one greater than the index of the last valid byte in
     * the buffer.
     */
    protected int count;

    /**
     * The current position in the buffer. This is the index of the next
     * character to be read from the <code>buf</code> array.
     */
    protected int pos;

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   bufferSize              the stream buffer size.
     * @param   ehcacheStreamReader     the stream reader implementation
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public EhcacheBufferedInputStream(int bufferSize, EhcacheStreamReader ehcacheStreamReader) throws EhcacheStreamException {
        super(ehcacheStreamReader);

        if (bufferSize <= 0) {
            throw new EhcacheStreamIllegalArgumentException("Buffer size <= 0");
        }
        this.buf = new byte[bufferSize];
    }

    @Override
    public int available() throws EhcacheStreamException {
        return ehcacheStreamReader.available();
    }

    /**
     * Check to make sure that buffer has not been nulled out due to
     * close; if not return it;
     */
    private byte[] getBufIfOpen() throws EhcacheStreamException {
        byte[] buffer = buf;
        if (buffer == null)
            throw new EhcacheStreamException("Stream closed");
        return buffer;
    }

    /**
     * Fills the buffer with more data
     * Assumes that it is being called by a synchronized method.
     * This method also assumes that all data has already been read in,
     * hence pos > count.
     */
    private void fill() throws EhcacheStreamException {
        byte[] buffer = getBufIfOpen();

        /* if we're here, that means we need to refill the buffer and as such it's ok to throw away the content of the buffer */
        pos = 0;
        count = pos;

        int byteCopied = ehcacheStreamReader.read(buffer, pos, buffer.length - pos);

        if (byteCopied > 0)
            count = pos + byteCopied;
    }

    /**
     * See
     * the general contract of the <code>read</code>
     * method of <code>InputStream</code>.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     * @see        java.io.FilterInputStream#in
     */
    public synchronized int read() throws EhcacheStreamException {
        if (pos >= count) {
            fill();
            if (pos >= count)
                return -1;
        }
        return getBufIfOpen()[pos++] & 0xff;
    }

    /**
     * Read characters into a portion of an array, reading from the underlying
     * stream at most once if necessary.
     */
    private int read1(byte[] b, int off, int len) throws EhcacheStreamException {
        //bytes available for reading in the buffer
        int avail = count - pos;
        if (avail <= 0) {
            fill();
            avail = count - pos;
            if (avail <= 0)
                return -1;
        }

        //check if length requested is bigger than number of available readable bytes
        int cnt = (avail < len) ? avail : len;
        System.arraycopy(getBufIfOpen(), pos, b, off, cnt);
        pos += cnt;
        return cnt;
    }

    /**
     * Reads bytes from this byte-input stream into the specified byte array,
     * starting at the given offset.
     *
     * <p> This method implements the general contract of the corresponding
     * <code>{@link InputStream#read(byte[], int, int) read}</code> method of
     * the <code>{@link InputStream}</code> class.  As an additional
     * convenience, it attempts to read as many bytes as possible by repeatedly
     * invoking the <code>read</code> method of the underlying stream.  This
     * iterated <code>read</code> continues until one of the following
     * conditions becomes true: <ul>
     *
     *   <li> The specified number of bytes have been read,
     *
     *   <li> The <code>read</code> method of the underlying stream returns
     *   <code>-1</code>, indicating end-of-file
     *
     * </ul> If the first <code>read</code> on the underlying stream returns
     * <code>-1</code> to indicate end-of-file then this method returns
     * <code>-1</code>.  Otherwise this method returns the number of bytes
     * actually read.
     *
     * @param      b     destination buffer.
     * @param      off   offset at which to start storing bytes.
     * @param      len   maximum number of bytes to read.
     * @return     the number of bytes read, or <code>-1</code> if the end of
     *             the stream has been reached.
     * @exception  IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     */
    @Override
    public synchronized int read(byte b[], int off, int len)
            throws EhcacheStreamException
    {
        getBufIfOpen(); // Check for closed stream
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        //total number of bytes read
        int n = 0;
        for (;;) {
            int nread = read1(b, off + n, len - n);
            if (nread <= 0)
                return (n == 0) ? nread : n;
            n += nread;
            if (n >= len)
                return n;
        }
    }

    /**
     * Closes this input stream and releases any system resources
     * associated with the stream.
     * Once the stream has been closed, further read(), available(), reset(),
     * or skip() invocations will throw an IOException.
     * Closing a previously closed stream has no effect.
     *
     * @exception  IOException  if an I/O error occurs.
     */
    public void close() throws EhcacheStreamException {
        try {
            byte[] buffer;
            while ((buffer = buf) != null) {
                if (bufUpdater.compareAndSet(this, buffer, null)) {
                    return;
                }
                // Else retry in case a new buf was CASed in fill()
            }
        } finally {
            ehcacheStreamReader.close();
        }
    }
}
