package org.ehcache.extensions.io.impl.utils;

import net.sf.ehcache.Ehcache;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public class EhcacheStreamUtilsFactory {
    /**
     * Get an IEhcacheStreamUtils object backed by Ehcache.
     *
     * @return    an IEhcacheStreamUtils object
     */
    public static IEhcacheStreamUtils getUtils(Ehcache cache) {
        return new EhcacheStreamUtilsPublicImpl(cache);
    }
}
