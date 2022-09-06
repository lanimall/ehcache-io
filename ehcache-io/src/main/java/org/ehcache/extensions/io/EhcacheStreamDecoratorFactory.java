package org.ehcache.extensions.io;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.constructs.CacheDecoratorFactory;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Created by fabien.sanglier on 9/17/18.
 */
public class EhcacheStreamDecoratorFactory extends CacheDecoratorFactory {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamDecoratorFactory.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    private static final String PROPNAME_PUTS_BUFFERSIZE = "puts_buffersize";
    private static final String PROPNAME_GETS_BUFFERSIZE = "gets_buffersize";

    private static final String PROPNAME_PUTS_USE_OVERWRITES = "puts_overwrite";
    private static final String PROPNAME_GETS_AS_BYTES = "gets_asbytes";
    private static final String PROPNAME_DECORATED_NAME = "cachename";

    private static final int DEFAULT_GETS_BUFFERSIZE = PropertyUtils.getInputStreamBufferSize();
    private static final int DEFAULT_PUTS_BUFFERSIZE = PropertyUtils.getOutputStreamBufferSize();
    private static final boolean DEFAULT_PUTS_USE_OVERWRITES = PropertyUtils.getOutputStreamDefaultOverride();
    private static final boolean DEFAULT_GETS_AS_BYTES = false;

    @Override
    public Ehcache createDecoratedEhcache(Ehcache ehcache, Properties properties) {
        if(isDebug) {
            logger.debug("Creating decorated cache with decorator implementation :" + EhcacheStreamDecorator.class);

            if(null != properties) {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry prop : properties.entrySet()) {
                    if (sb.length() > 0)
                        sb.append(";");
                    sb.append(prop.getKey()).append("=").append(prop.getValue());
                }
                logger.debug("Decorator properties : {}", EhcacheStreamUtilsInternal.toStringSafe(sb));
            }
        }

        EhcacheStreamDecorator ehcacheStreamDecorator = new EhcacheStreamDecorator(
                ehcache,
                PropertyUtils.getPropertyAsBoolean(properties, PROPNAME_PUTS_USE_OVERWRITES, DEFAULT_PUTS_USE_OVERWRITES),
                PropertyUtils.getPropertyAsInt(properties, PROPNAME_PUTS_BUFFERSIZE, DEFAULT_PUTS_BUFFERSIZE),
                PropertyUtils.getPropertyAsInt(properties, PROPNAME_GETS_BUFFERSIZE, DEFAULT_GETS_BUFFERSIZE),
                PropertyUtils.getPropertyAsBoolean(properties, PROPNAME_GETS_AS_BYTES, DEFAULT_GETS_AS_BYTES)
        );

        String decoratedCacheName = PropertyUtils.getPropertyAsString(properties, PROPNAME_DECORATED_NAME, null);
        if(null != decoratedCacheName)
            ehcacheStreamDecorator.setName(decoratedCacheName);

        return ehcacheStreamDecorator;
    }

    @Override
    public Ehcache createDefaultDecoratedEhcache(Ehcache ehcache, Properties properties) {
        throw new UnsupportedOperationException();
    }
}
