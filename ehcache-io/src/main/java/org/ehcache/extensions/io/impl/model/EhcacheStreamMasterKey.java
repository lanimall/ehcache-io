package org.ehcache.extensions.io.impl.model;

import java.io.Serializable;

/**
 * Created by fabien.sanglier on 9/28/18.
 */
public class EhcacheStreamMasterKey implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final Object cacheKey;

    public EhcacheStreamMasterKey(Object cacheKey) {
        this.cacheKey = cacheKey;
    }

    public Object getCacheKey() {
        return cacheKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamMasterKey that = (EhcacheStreamMasterKey) o;

        if (cacheKey != null ? !cacheKey.equals(that.cacheKey) : that.cacheKey != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return cacheKey != null ? cacheKey.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "EhcacheStreamMasterKey{" +
                "cacheKey=" + cacheKey +
                '}' +
                ", hashcode=" + hashCode();
    }
}
