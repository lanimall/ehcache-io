package org.ehcache.extensions.io;

/**
 * Created by fabien.sanglier on 10/10/18.
 */
public class EhcacheStreamIllegalArgumentException extends IllegalArgumentException {
    public EhcacheStreamIllegalArgumentException() {
    }

    public EhcacheStreamIllegalArgumentException(String s) {
        super(s);
    }

    public EhcacheStreamIllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public EhcacheStreamIllegalArgumentException(Throwable cause) {
        super(cause);
    }
}
