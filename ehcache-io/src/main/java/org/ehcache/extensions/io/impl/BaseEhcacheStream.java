package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public abstract class BaseEhcacheStream {

    private final EhcacheStreamUtilsInternal ehcacheStreamUtils;

    private final Object cacheKey;

    protected BaseEhcacheStream(Ehcache cache, Object cacheKey) {
        this.ehcacheStreamUtils = new EhcacheStreamUtilsInternal(cache);
        this.cacheKey = cacheKey;
    }

    public EhcacheStreamUtilsInternal getEhcacheStreamUtils() {
        return ehcacheStreamUtils;
    }

    public Object getCacheKey() {
        return cacheKey;
    }
}