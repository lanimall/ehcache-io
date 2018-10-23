package org.ehcache.extensions.io.impl.readers;

import org.ehcache.extensions.io.EhcacheStreamException;

import java.io.Closeable;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public interface EhcacheStreamReader extends Closeable {
    boolean isOpen();

    int available() throws EhcacheStreamException;

    int read() throws EhcacheStreamException;

    int read(byte[] b, int off, int len) throws EhcacheStreamException;

    void open() throws EhcacheStreamException;

    public void close() throws EhcacheStreamException;
}
