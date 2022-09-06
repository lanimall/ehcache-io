package org.ehcache.extensions.io;

/**
 * Created by fabien.sanglier on 7/27/18.
 */
public class EhcacheStreamIllegalStateException extends IllegalStateException {

    public EhcacheStreamIllegalStateException() {
    }

    public EhcacheStreamIllegalStateException(String message) {
        super(message);
    }

    public EhcacheStreamIllegalStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public EhcacheStreamIllegalStateException(Throwable cause) {
        super(cause);
    }
}
