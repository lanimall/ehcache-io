package org.ehcache.extensions.io;

import java.io.IOException;

/**
 * Created by fabien.sanglier on 7/27/18.
 */
public class EhcacheStreamException extends IOException {

    public EhcacheStreamException() {
    }

    public EhcacheStreamException(String message) {
        super(message);
    }

    public EhcacheStreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public EhcacheStreamException(Throwable cause) {
        super(cause);
    }
}
