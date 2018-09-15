package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;

import java.io.InputStream;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public class EhcacheStreamReadersFactory {
    /**
     * Get an IEhcacheStreamReader object backed by Ehcache.
     *
     * @return    a valid IEhcacheStreamReader object
     */
    public static EhcacheStreamReader getReader(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
        EhcacheStreamReader ehcacheStreamReader;
        switch (PropertyUtils.ehcacheIOStreamsConcurrencyMode){
            case READ_COMMITTED_WITHLOCKS:
                ehcacheStreamReader = new EhcacheStreamReaderWithSingleLock(cache, cacheKey, openTimeoutMillis);
                break;
            case WRITE_PRIORITY_NOLOCK:
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
