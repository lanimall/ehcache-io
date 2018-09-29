package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public abstract class BaseEhcacheStream {

    private final EhcacheStreamUtilsInternal ehcacheStreamUtils;

    private final Object publicCacheKey;

    protected BaseEhcacheStream(Ehcache cache, Object publicCacheKey) {
        this.ehcacheStreamUtils = new EhcacheStreamUtilsInternal(cache);
        this.publicCacheKey = publicCacheKey;
    }

    public EhcacheStreamUtilsInternal getEhcacheStreamUtils() {
        return ehcacheStreamUtils;
    }

    public Object getPublicCacheKey() {
        return publicCacheKey;
    }
}