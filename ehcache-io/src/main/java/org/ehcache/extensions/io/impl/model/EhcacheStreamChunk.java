package org.ehcache.extensions.io.impl.model;

import java.io.Serializable;
import java.util.Arrays;

/**
* Created by Fabien Sanglier on 5/6/15.
*/

public class EhcacheStreamChunk implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] chunk;

    public EhcacheStreamChunk(byte[] chunk) {
        this.chunk = chunk;
    }

    public byte[] getChunk() {
        return chunk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EhcacheStreamChunk that = (EhcacheStreamChunk) o;

        if (!Arrays.equals(chunk, that.chunk)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return chunk != null ? Arrays.hashCode(chunk) : 0;
    }

    @Override
    public String toString() {
        return "EhcacheStreamChunk{" +
                "chunk=" + ((null!= chunk)?chunk.length + " bytes":"null") +
                '}' +
                ", hashcode=" + hashCode();
    }
}