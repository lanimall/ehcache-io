package org.ehcache.extensions.io.impl.utils;

/**
 * Created by fabien.sanglier on 9/18/18.
 */
public interface WaitStrategy {
    void doWait(final long attempt);
}
