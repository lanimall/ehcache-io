package org.ehcache.extensions.io;

import net.sf.ehcache.Cache;
import org.ehcache.extensions.io.impl.EhcacheStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Created by Fabien Sanglier on 5/4/15.
 */
public class EhcacheInputStream extends InputStream {
    private static int DEFAULT_BUFFER_SIZE = 512 * 1024; // 512kb

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
    AtomicReferenceFieldUpdater<EhcacheInputStream, byte[]> bufUpdater =
            AtomicReferenceFieldUpdater.newUpdater
                    (EhcacheInputStream.class,  byte[].class, "buf");

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

    /*
     * The Internal Ehcache streaming access layer
     */
    protected final EhcacheStreamReader ehcacheStreamReader;

    /**
     * Creates a new Ehcache Input Stream to read data from a cache key
     *
     * @param   cache       the underlying cache to access
     * @param   cacheKey    the underlying cache key to read data from
     */
    public EhcacheInputStream(Cache cache, Object cacheKey) {
        this.buf = new byte[DEFAULT_BUFFER_SIZE];
        this.ehcacheStreamReader = new EhcacheStreamReader(cache,cacheKey);
    }

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   cache       the underlying cache to access
     * @param   cacheKey    the underlying cache key to read data from
     * @param   size        the buffer size.
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public EhcacheInputStream(Cache cache, Object cacheKey, int size, boolean lockReadsImmediately) throws IOException {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        this.buf = new byte[size];
        this.ehcacheStreamReader = new EhcacheStreamReader(cache,cacheKey);

        if(lockReadsImmediately) {
            try {
                ehcacheStreamReader.open();
            } catch (InterruptedException e) {
                throw new IOException("Could not acquire the read lock for the underlying datastore", e);
            }
        }
    }

    /**
     * Check to make sure that buffer has not been nulled out due to
     * close; if not return it;
     */
    private byte[] getBufIfOpen() throws IOException {
        byte[] buffer = buf;
        if (buffer == null)
            throw new IOException("Stream closed");
        return buffer;
    }

    /**
     * Fills the buffer with more data
     * Assumes that it is being called by a synchronized method.
     * This method also assumes that all data has already been read in,
     * hence pos > count.
     */
    private void fill() throws IOException {
        byte[] buffer = getBufIfOpen();

        /* if we're here, that means we need to refill the buffer and as such it's ok to throw away the content of the buffer */
        pos = 0;
        count = pos;

        int byteCopied;
        try {
            byteCopied = ehcacheStreamReader.read(buffer, pos);
        } catch (InterruptedException e) {
            throw new IOException("Could not acquire the read lock within the timeout", e);
        }

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
    public synchronized int read() throws IOException {
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
    private int read1(byte[] b, int off, int len) throws IOException {
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
            throws IOException
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
    public void close() throws IOException {
        try {
            byte[] buffer;
            while ((buffer = buf) != null) {
                if (bufUpdater.compareAndSet(this, buffer, null)) {
                    return;
                }
                // Else retry in case a new buf was CASed in fill()
            }
        } finally {
            //close the ehcache reader
            ehcacheStreamReader.close();
        }
    }
}
