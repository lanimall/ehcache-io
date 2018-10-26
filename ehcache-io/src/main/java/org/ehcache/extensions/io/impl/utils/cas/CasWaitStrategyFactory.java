package org.ehcache.extensions.io.impl.utils.cas;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.impl.utils.EhcacheUtils;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;

/**
 * Created by fabien.sanglier on 10/25/18.
 */
public class CasWaitStrategyFactory {
    //cap of 100ms achieved by attempt 7 (but jitters will mean that it'll be a value between 0 and 100)
    public static final long DEFAULT_BASE_MILLIS = 1;
    public static final long DEFAULT_CAP_MILLIS = 100;
    public static final boolean DEFAULT_JITTER = true;

    //local heap only
    public static final long DEFAULT_LOCAL_HEAPONLY_BASE_MILLIS = 1;
    public static final long DEFAULT_LOCAL_HEAPONLY_CAP_MILLIS = 20;
    public static final boolean DEFAULT_LOCAL_HEAPONLY_JITTER = true;

    //local with offheap
    public static final long DEFAULT_LOCAL_HEAPOFFHEAP_BASE_MILLIS = 1;
    public static final long DEFAULT_LOCAL_HEAPOFFHEAP_CAP_MILLIS = 100;
    public static final boolean DEFAULT_LOCAL_HEAPOFFHEAP_JITTER = true;

    //clustered local enabled
    public static final long DEFAULT_CLUSTERED_LOCAL_ENABLED_BASE_MILLIS = 1;
    public static final long DEFAULT_CLUSTERED_LOCAL_ENABLED_CAP_MILLIS = 500;
    public static final boolean DEFAULT_CLUSTERED_LOCAL_ENABLED_JITTER = true;

    //clustered no local
    public static final long DEFAULT_CLUSTERED_NOLOCAL_BASE_MILLIS = 10;
    public static final long DEFAULT_CLUSTERED_NOLOCAL_CAP_MILLIS = 1000;
    public static final boolean DEFAULT_CLUSTERED_NOLOCAL_JITTER = true;

    public static WaitStrategy getWaitStrategy(Ehcache cache) {
        WaitStrategy waitStrategy;

        EhcacheUtils.CacheType cacheType = EhcacheUtils.CacheType.findCacheType(cache);
        switch (cacheType){
            case CLUSTERED_EVENTUAL_LOCAL_ENABLED:
            case CLUSTERED_STRONG_LOCAL_ENABLED:
            case CLUSTERED_LOCAL_ENABLED: //clustered local enabled
                waitStrategy = new ExponentialWait(
                        PropertyUtils.getCasLoopExponentialBackoffBase(DEFAULT_CLUSTERED_LOCAL_ENABLED_BASE_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffCap(DEFAULT_CLUSTERED_LOCAL_ENABLED_CAP_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffUseJitter(DEFAULT_CLUSTERED_LOCAL_ENABLED_JITTER)
                );
                break;
            case CLUSTERED_NOLOCAL: //clustered local disabled
                waitStrategy = new ExponentialWait(
                        PropertyUtils.getCasLoopExponentialBackoffBase(DEFAULT_CLUSTERED_NOLOCAL_BASE_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffCap(DEFAULT_CLUSTERED_NOLOCAL_CAP_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffUseJitter(DEFAULT_CLUSTERED_NOLOCAL_JITTER)
                );
                break;
            case LOCAL_HEAP_OFFHEAP: //local heap + offheap
                waitStrategy = new ExponentialWait(
                        PropertyUtils.getCasLoopExponentialBackoffBase(DEFAULT_LOCAL_HEAPOFFHEAP_BASE_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffCap(DEFAULT_LOCAL_HEAPOFFHEAP_CAP_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffUseJitter(DEFAULT_LOCAL_HEAPOFFHEAP_JITTER)
                );
                break;
            case LOCAL_ONLY:
            case LOCAL_HEAP_ONLY: //local heap
                waitStrategy = new ExponentialWait(
                        PropertyUtils.getCasLoopExponentialBackoffBase(DEFAULT_LOCAL_HEAPONLY_BASE_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffCap(DEFAULT_LOCAL_HEAPONLY_CAP_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffUseJitter(DEFAULT_LOCAL_HEAPONLY_JITTER)
                );
                break;
            default:
                waitStrategy = new ExponentialWait(
                        PropertyUtils.getCasLoopExponentialBackoffBase(DEFAULT_BASE_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffCap(DEFAULT_CAP_MILLIS),
                        PropertyUtils.getCasLoopExponentialBackoffUseJitter(DEFAULT_JITTER)
                );
                break;
        }

        return waitStrategy;
    }
}

