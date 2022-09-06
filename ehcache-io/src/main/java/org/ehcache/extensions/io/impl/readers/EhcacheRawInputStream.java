package org.ehcache.extensions.io.impl.readers;

import org.ehcache.extensions.io.EhcacheStreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 10/23/18.
 */
/*package protected*/ class EhcacheRawInputStream extends EhcacheInputStream {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheRawInputStream.class);

    EhcacheRawInputStream(EhcacheStreamReader ehcacheStreamReader) throws EhcacheStreamException {
        super(ehcacheStreamReader);
    }

    @Override
    public int read() throws EhcacheStreamException {
        return ehcacheStreamReader.read();
    }

    @Override
    public int available() throws EhcacheStreamException {
        return ehcacheStreamReader.available();
    }

    @Override
    public int read(byte[] b, int off, int len) throws EhcacheStreamException {
        return ehcacheStreamReader.read(b, off, len);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void close() throws EhcacheStreamException {
        ehcacheStreamReader.close();
    }
}
