package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public abstract class BaseEhcacheStream {

    protected final EhcacheStreamUtils ehcacheStreamUtils;

    protected BaseEhcacheStream(Cache cache, Object cacheKey) {
        this.ehcacheStreamUtils = new EhcacheStreamUtils(cache, cacheKey);
    }
}