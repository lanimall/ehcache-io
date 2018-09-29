package org.ehcache.extensions.io.impl.model;

import java.io.Serializable;

/**
* Created by Fabien Sanglier on 5/6/15.
*/

public class EhcacheStreamChunkKey extends EhcacheStreamMasterKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int chunkIndex;

    public EhcacheStreamChunkKey(Object cacheKey, int chunkIndex) {
        super(cacheKey);
        this.chunkIndex = chunkIndex;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        EhcacheStreamChunkKey that = (EhcacheStreamChunkKey) o;

        if (chunkIndex != that.chunkIndex) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + chunkIndex;
        return result;
    }

    @Override
    public String toString() {
        return "EhcacheStreamChunkKey{" +
                "cacheKey=" + ((null != cacheKey)?cacheKey.toString():"null") +
                ", chunkIndex=" + chunkIndex +
                '}' +
                ", hashcode=" + hashCode();
    }
}
