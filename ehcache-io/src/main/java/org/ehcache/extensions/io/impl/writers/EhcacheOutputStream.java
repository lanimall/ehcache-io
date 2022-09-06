package org.ehcache.extensions.io.impl.writers;

import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

/**
 * Created by fabien.sanglier on 10/25/18.
 */
public abstract class EhcacheOutputStream extends OutputStream {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheOutputStream.class);

    /**
     * The Internal Ehcache streaming access layer
     */
    protected final EhcacheStreamWriter ehcacheStreamWriter;

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param   ehcacheStreamWriter   the stream writer implementation
     *
     * @exception org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException if size &lt;= 0 or if ehcacheStreamWriter is null
     * @exception org.ehcache.extensions.io.EhcacheStreamException if ehcacheStreamWriter could not be opened
     */
    protected EhcacheOutputStream(EhcacheStreamWriter ehcacheStreamWriter) throws EhcacheStreamException {
        if (null == ehcacheStreamWriter) {
            throw new EhcacheStreamIllegalArgumentException("An internal stream writer must be provided");
        }

        this.ehcacheStreamWriter = ehcacheStreamWriter;

        try {
            this.ehcacheStreamWriter.tryOpen();
        } catch (EhcacheStreamException e) {
            //silent close just to make sure we cleanup a possible half open
            try {
                this.ehcacheStreamWriter.close();
            } catch (Exception e1) {
                logger.error("Error during internal stream reader close", e1);
            }

            //bubble up the exception
            throw e;
        }
    }

    public Object getPublicCacheKey(){
        return ehcacheStreamWriter.getPublicCacheKey();
    }
}
