package org.ehcache.extensions.io;

import java.io.Serializable;

/**
* Created by Fabien Sanglier on 5/6/15.
*/
class EhcacheStreamKey implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final int MASTER_INDEX = -1;

    private final Object cacheKey;
    private final int chunkIndex;

    EhcacheStreamKey(Object cacheKey, int chunkIndex) {
        this.cacheKey = cacheKey;
        this.chunkIndex = chunkIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamKey that = (EhcacheStreamKey) o;

        if (chunkIndex != that.chunkIndex) return false;
        if (cacheKey != null ? !cacheKey.equals(that.cacheKey) : that.cacheKey != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = cacheKey != null ? cacheKey.hashCode() : 0;
        result = 31 * result + chunkIndex;
        return result;
    }

    @Override
    public String toString() {
        return "InnerCacheKey{" +
                "cacheKey=" + cacheKey +
                ", chunkIndex=" + chunkIndex +
                '}';
    }
}
