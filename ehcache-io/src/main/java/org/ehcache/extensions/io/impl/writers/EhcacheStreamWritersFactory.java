package org.ehcache.extensions.io.impl.writers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public class EhcacheStreamWritersFactory {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamWritersFactory.class);

    /**
     * Get an IEhcacheStreamWriter object backed by Ehcache.
     *
     * @return    a valid IEhcacheStreamWriter object
     */
    public static EhcacheStreamWriter getWriter(Ehcache cache, Object cacheKey, boolean override, long openTimeoutMillis) {
        EhcacheStreamWriter ehcacheStreamWriter;
        PropertyUtils.ConcurrencyMode concurrencyMode = PropertyUtils.getEhcacheIOStreamsConcurrencyMode();
        if(logger.isDebugEnabled())
            logger.debug("Creating a stream writer with Concurrency mode: {}", concurrencyMode.getPropValue());

        switch (concurrencyMode){
            case WRITE_PRIORITY:
            case READ_COMMITTED_CASLOCKS:
                ehcacheStreamWriter = new EhcacheStreamWriterCasLock(cache, cacheKey, override, openTimeoutMillis);
                break;
            case READ_COMMITTED_WITHLOCKS:
                ehcacheStreamWriter = new EhcacheStreamWriterWithSingleLock(cache, cacheKey, override, openTimeoutMillis);
                break;
            default:
                throw new IllegalStateException("Not implemented");
        }

        return ehcacheStreamWriter;
    }

    /**
     * Get an OutputStream object backed by Ehcache.
     *
     * @return    a valid OutputStream object
     */
    public static OutputStream getStream(Ehcache cache, Object cacheKey, int streamBufferSize, boolean override, long openTimeoutMillis) throws EhcacheStreamException {
        return new EhcacheOutputStream(streamBufferSize, getWriter(cache, cacheKey, override, openTimeoutMillis));
    }
}
