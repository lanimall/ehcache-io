package org.ehcache.extensions.io.impl.utils;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.config.TerracottaConfiguration;
import org.ehcache.extensions.io.EhcacheStreamIllegalArgumentException;

/**
 * Created by fabien.sanglier on 10/25/18.
 */
public class EhcacheUtils {

    public static CacheType getCacheType(Ehcache cache){
        return CacheType.findCacheType(cache);
    }

    public enum CacheType {
        LOCAL_ONLY {
            @Override
            public boolean isCacheType(Ehcache cache) {
                TerracottaConfiguration tcConfig = cache.getCacheConfiguration().getTerracottaConfiguration();
                return tcConfig == null || !tcConfig.isClustered();
            }
        },
        LOCAL_HEAP_ONLY {
            @Override
            public boolean isCacheType(Ehcache cache) {
                return LOCAL_ONLY.isCacheType(cache)
                        && !cache.getCacheConfiguration().isOverflowToOffHeap()
                        && !cache.getCacheConfiguration().isOverflowToDisk();
            }
        },
        LOCAL_HEAP_OFFHEAP {
            @Override
            public boolean isCacheType(Ehcache cache) {
                return LOCAL_ONLY.isCacheType(cache) && cache.getCacheConfiguration().isOverflowToOffHeap();
            }
        },
        CLUSTERED {
            @Override
            public boolean isCacheType(Ehcache cache) {
                TerracottaConfiguration tcConfig = cache.getCacheConfiguration().getTerracottaConfiguration();
                return tcConfig != null && tcConfig.isClustered();
            }
        },
        CLUSTERED_NOLOCAL {
            @Override
            public boolean isCacheType(Ehcache cache) {
                TerracottaConfiguration tcConfig = cache.getCacheConfiguration().getTerracottaConfiguration();
                return CLUSTERED.isCacheType(cache) && !tcConfig.isLocalCacheEnabled();
            }
        },
        CLUSTERED_LOCAL_ENABLED {
            @Override
            public boolean isCacheType(Ehcache cache) {
                TerracottaConfiguration tcConfig = cache.getCacheConfiguration().getTerracottaConfiguration();
                return CLUSTERED.isCacheType(cache) && tcConfig.isLocalCacheEnabled();
            }
        },
        CLUSTERED_STRONG_LOCAL_ENABLED {
            @Override
            public boolean isCacheType(Ehcache cache) {
                TerracottaConfiguration tcConfig = cache.getCacheConfiguration().getTerracottaConfiguration();
                return CLUSTERED_LOCAL_ENABLED.isCacheType(cache) && tcConfig.getConsistency() == TerracottaConfiguration.Consistency.STRONG;
            }
        },
        CLUSTERED_EVENTUAL_LOCAL_ENABLED {
            @Override
            public boolean isCacheType(Ehcache cache) {
                TerracottaConfiguration tcConfig = cache.getCacheConfiguration().getTerracottaConfiguration();
                return CLUSTERED_LOCAL_ENABLED.isCacheType(cache) && tcConfig.getConsistency() == TerracottaConfiguration.Consistency.EVENTUAL;
            }
        };

        public abstract boolean isCacheType(Ehcache cache);

        public static CacheType findCacheType(Ehcache cache){
            CacheType cacheType;

            if(null == cache)
                throw new EhcacheStreamIllegalArgumentException("Cache null is not valid");

            if(CLUSTERED_STRONG_LOCAL_ENABLED.isCacheType(cache)){
                cacheType = CLUSTERED_STRONG_LOCAL_ENABLED;
            } else if(CLUSTERED_EVENTUAL_LOCAL_ENABLED.isCacheType(cache)) {
                cacheType = CLUSTERED_EVENTUAL_LOCAL_ENABLED;
            } else if(CLUSTERED_NOLOCAL.isCacheType(cache)){
                cacheType = CLUSTERED_NOLOCAL;
            } else if(LOCAL_HEAP_OFFHEAP.isCacheType(cache)){
                cacheType = LOCAL_HEAP_OFFHEAP;
            } else if (LOCAL_HEAP_ONLY.isCacheType(cache)){
                cacheType = LOCAL_HEAP_ONLY;
            } else if (CLUSTERED_LOCAL_ENABLED.isCacheType(cache)){
                cacheType = CLUSTERED_LOCAL_ENABLED;
            } else if (CLUSTERED.isCacheType(cache)){
                cacheType = CLUSTERED;
            } else if (LOCAL_ONLY.isCacheType(cache)){
                cacheType = LOCAL_ONLY;
            } else {
                throw new EhcacheStreamIllegalArgumentException("Could not find a specific CacheType based on is not valid");
            }

            return cacheType;
        }
    }
}
