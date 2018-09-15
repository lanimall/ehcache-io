package org.ehcache.extensions.io.impl.writers;

import org.ehcache.extensions.io.EhcacheStreamException;

import java.io.Closeable;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public interface EhcacheStreamWriter extends Closeable {
    void tryOpen() throws EhcacheStreamException;

    void close() throws EhcacheStreamException;

    void writeData(byte[] buf, int count) throws EhcacheStreamException;
}
