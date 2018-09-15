package org.ehcache.extensions.io.impl.model;

import java.io.Serializable;

/**
* Created by Fabien Sanglier on 5/6/15.
*/

public class EhcacheStreamValue implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] chunk;

    public EhcacheStreamValue(byte[] chunk) {
        this.chunk = chunk;
    }

    public byte[] getChunk() {
        return chunk;
    }
}