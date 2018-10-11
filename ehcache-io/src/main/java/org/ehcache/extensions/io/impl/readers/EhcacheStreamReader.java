package org.ehcache.extensions.io.impl.readers;

import org.ehcache.extensions.io.EhcacheStreamException;

import java.io.Closeable;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public interface EhcacheStreamReader extends Closeable {
    int getSize();

    void tryOpen() throws EhcacheStreamException;

    int read(byte[] outBuf, int bufferBytePos) throws EhcacheStreamException;

    void close() throws EhcacheStreamException;
}
