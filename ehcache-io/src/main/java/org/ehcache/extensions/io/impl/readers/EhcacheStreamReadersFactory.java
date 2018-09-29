package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public class EhcacheStreamReadersFactory {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamReadersFactory.class);

    /**
     * Get an IEhcacheStreamReader object backed by Ehcache.
     *
     * @return    a valid IEhcacheStreamReader object
     */
    public static EhcacheStreamReader getReader(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
        EhcacheStreamReader ehcacheStreamReader;
        PropertyUtils.ConcurrencyMode concurrencyMode = PropertyUtils.getEhcacheIOStreamsConcurrencyMode();
        if(logger.isDebugEnabled())
            logger.debug("Creating a stream reader with Concurrency mode: {}", EhcacheStreamUtilsInternal.toStringSafe(concurrencyMode));

        switch (concurrencyMode){
            case READ_COMMITTED_CASLOCKS:
                ehcacheStreamReader = new EhcacheStreamReaderCasLock(cache, cacheKey, openTimeoutMillis);
                break;
            case READ_COMMITTED_WITHLOCKS:
                ehcacheStreamReader = new EhcacheStreamReaderWithSingleLock(cache, cacheKey, openTimeoutMillis);
                break;
            case WRITE_PRIORITY:
                ehcacheStreamReader = new EhcacheStreamReaderNoLock(cache, cacheKey, openTimeoutMillis);
                break;
            default:
                throw new IllegalStateException("Not implemented");
        }

        return ehcacheStreamReader;
    }

    /**
     * Get an InputStream object backed by Ehcache.
     *
     * @return    a valid InputStream object
     */
    public static InputStream getStream(Ehcache cache, Object cacheKey, int streamBufferSize, long openTimeoutMillis) throws EhcacheStreamException {
        return new EhcacheInputStream(streamBufferSize, getReader(cache, cacheKey, openTimeoutMillis));
    }
}
