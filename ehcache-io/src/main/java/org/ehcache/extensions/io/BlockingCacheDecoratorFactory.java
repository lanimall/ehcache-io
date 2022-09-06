package org.ehcache.extensions.io;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.constructs.CacheDecoratorFactory;
import net.sf.ehcache.constructs.blocking.BlockingCache;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;

import java.util.Properties;

/**
 * Created by fabien.sanglier on 10/30/18.
 */
public class BlockingCacheDecoratorFactory extends CacheDecoratorFactory {
    private static final int TIMEOUT_MILLIS = 1000;
    private static final String PROPNAME_DECORATED_NAME = "cachename";

    @Override
    public Ehcache createDecoratedEhcache(final Ehcache cache, final Properties properties) {
        final BlockingCache blockingCache = new BlockingCache(cache);
        blockingCache.setTimeoutMillis(TIMEOUT_MILLIS);

        String decoratedCacheName = PropertyUtils.getPropertyAsString(properties, PROPNAME_DECORATED_NAME, null);
        if(null != decoratedCacheName)
            blockingCache.setName(decoratedCacheName);

        return blockingCache;
    }

    @Override
    public Ehcache createDefaultDecoratedEhcache(final Ehcache cache, final Properties properties) {
        return this.createDecoratedEhcache(cache, properties);
    }
}