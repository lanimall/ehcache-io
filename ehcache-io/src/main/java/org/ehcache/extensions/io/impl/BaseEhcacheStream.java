package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public abstract class BaseEhcacheStream {

    private final EhcacheStreamUtils ehcacheStreamUtils;

    private final Object cacheKey;

    protected BaseEhcacheStream(Ehcache cache, Object cacheKey) {
        this.ehcacheStreamUtils = new EhcacheStreamUtils(cache);
        this.cacheKey = cacheKey;
    }

    public EhcacheStreamUtils getEhcacheStreamUtils() {
        return ehcacheStreamUtils;
    }

    public Object getCacheKey() {
        return cacheKey;
    }
}