package org.ehcache.extensions.io.impl.readers;

import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by fabien.sanglier on 10/25/18.
 */
public abstract class EhcacheInputStream extends InputStream {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheInputStream.class);

    /*
         * The Internal Ehcache streaming access layer
         */
    protected final EhcacheStreamReader ehcacheStreamReader;

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   ehcacheStreamReader     the stream reader implementation
     * @exception org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException if ehcacheStreamReader is null.
     * @exception org.ehcache.extensions.io.EhcacheStreamException if ehcacheStreamReader could not be opened
     */
    protected EhcacheInputStream(EhcacheStreamReader ehcacheStreamReader) throws EhcacheStreamException {
        if (null == ehcacheStreamReader) {
            throw new EhcacheStreamIllegalArgumentException("An internal stream reader must be provided");
        }
        this.ehcacheStreamReader = ehcacheStreamReader;

        try {
            this.ehcacheStreamReader.open();
        } catch (EhcacheStreamException e) {
            //silent close just to make sure we cleanup a possible half open
            try {
                this.ehcacheStreamReader.close();
            } catch (Exception e1) {
                logger.error("Error during internal stream reader close", e1);
            }

            //bubble up the exception
            throw e;
        }
    }

    public Object getPublicCacheKey(){
        return ehcacheStreamReader.getPublicCacheKey();
    }

    @Override
    public abstract int read() throws EhcacheStreamException;

    @Override
    public int read(byte[] b) throws EhcacheStreamException {
        try {
            return super.read(b);
        } catch (IOException e) {
            throw new EhcacheStreamException(e);
        }
    }

    @Override
    public abstract int read(byte[] b, int off, int len) throws EhcacheStreamException;

    @Override
    public abstract int available() throws EhcacheStreamException;

    @Override
    public abstract void close() throws EhcacheStreamException;
}
