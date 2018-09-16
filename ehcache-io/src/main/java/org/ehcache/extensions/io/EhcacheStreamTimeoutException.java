package org.ehcache.extensions.io;

/**
 * Created by fabien.sanglier on 7/27/18.
 */
public class EhcacheStreamTimeoutException extends EhcacheStreamException {

    public EhcacheStreamTimeoutException() {
    }

    public EhcacheStreamTimeoutException(String message) {
        super(message);
    }

    public EhcacheStreamTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public EhcacheStreamTimeoutException(Throwable cause) {
        super(cause);
    }
}
