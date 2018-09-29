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

    private static final String PROPNAME_BUFFERSIZE = "buffersize";
    private static final String PROPNAME_USECOMPRESSION = "compression";
    private static final String PROPNAME_USE_OVERWRITES_PUTS = "puts_overwrite";
    private static final String PROPNAME_RETURNASSTREAM_GETS = "gets_returnasstream";

    private static final int DEFAULT_BUFFERSIZE = 512 * 1024;
    private static final boolean DEFAULT_USECOMPRESSION = false;
    private static final boolean DEFAULT_USE_OVERWRITES_PUTS = true;
    private static final boolean DEFAULT_RETURNASSTREAM_GETS = true;

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

        return new EhcacheStreamDecorator(
                ehcache,
                PropertyUtils.getPropertyAsInt(PROPNAME_BUFFERSIZE, DEFAULT_BUFFERSIZE),
                PropertyUtils.getPropertyAsBoolean(PROPNAME_USECOMPRESSION, DEFAULT_USECOMPRESSION),
                PropertyUtils.getPropertyAsBoolean(PROPNAME_USE_OVERWRITES_PUTS, DEFAULT_USE_OVERWRITES_PUTS),
                PropertyUtils.getPropertyAsBoolean(PROPNAME_RETURNASSTREAM_GETS, DEFAULT_RETURNASSTREAM_GETS),
                null
        );
    }

    @Override
    public Ehcache createDefaultDecoratedEhcache(Ehcache ehcache, Properties properties) {
        throw new UnsupportedOperationException();
    }
}
