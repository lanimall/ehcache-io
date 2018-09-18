package org.ehcache.extensions.io;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.constructs.CacheDecoratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by fabien.sanglier on 9/17/18.
 */
public class EhcacheStreamDecoratorFactory extends CacheDecoratorFactory {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamDecoratorFactory.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    private static final int DEFAULT_BUFFERSIZE = 512 * 1024;
    private static final boolean DEFAULT_USECOMPRESSION = false;
    private static final boolean DEFAULT_USE_OVERWRITES_PUTS = true;
    private static final boolean DEFAULT_RETURNASSTREAM_GETS = true;

    @Override
    public Ehcache createDecoratedEhcache(Ehcache ehcache, Properties properties) {
        if(isDebug)
            logger.debug("Creating decorated cache with:" + EhcacheStreamDecorator.class);

        return new EhcacheStreamDecorator(
                ehcache,
                DEFAULT_BUFFERSIZE,
                DEFAULT_USECOMPRESSION,
                DEFAULT_USE_OVERWRITES_PUTS,
                DEFAULT_RETURNASSTREAM_GETS,
                null
        );
    }

    @Override
    public Ehcache createDefaultDecoratedEhcache(Ehcache ehcache, Properties properties) {
        throw new UnsupportedOperationException();
    }
}
