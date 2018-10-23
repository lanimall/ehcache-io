package org.ehcache.extensions.io.impl.readers;

import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by fabien.sanglier on 10/23/18.
 */
/*package protected*/ class EhcacheRawInputStream extends InputStream {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheRawInputStream.class);

    /*
     * The Internal Ehcache streaming access layer
    */
    protected volatile EhcacheStreamReader ehcacheStreamReader;

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   ehcacheStreamReader     the stream reader implementation
     * @exception EhcacheStreamIllegalArgumentException if ehcacheStreamReader is null.
     * @exception EhcacheStreamException if ehcacheStreamReader could not be opened
     */
    public EhcacheRawInputStream(EhcacheStreamReader ehcacheStreamReader) throws EhcacheStreamException {
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

    @Override
    public int read() throws IOException {
        return ehcacheStreamReader.read();
    }

    @Override
    public int available() throws IOException {
        return ehcacheStreamReader.available();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return ehcacheStreamReader.read(b, off, len);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void close() throws IOException {
        ehcacheStreamReader.close();
    }
}
