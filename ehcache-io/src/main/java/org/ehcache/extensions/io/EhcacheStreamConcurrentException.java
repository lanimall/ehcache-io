package org.ehcache.extensions.io;

import java.io.IOException;

/**
 * Created by fabien.sanglier on 7/27/18.
 */
public class EhcacheStreamConcurrentException extends EhcacheStreamException {

    public EhcacheStreamConcurrentException() {
    }

    public EhcacheStreamConcurrentException(String message) {
        super(message);
    }

    public EhcacheStreamConcurrentException(String message, Throwable cause) {
        super(message, cause);
    }

    public EhcacheStreamConcurrentException(Throwable cause) {
        super(cause);
    }
}
