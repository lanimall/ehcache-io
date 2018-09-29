package org.ehcache.extensions.io.impl.utils;

import org.ehcache.extensions.io.EhcacheStreamException;

import java.util.List;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public interface IEhcacheStreamUtils {
    boolean removeStreamEntry(final Object cacheKey, long timeout) throws EhcacheStreamException;

    boolean containsStreamEntry(final Object cacheKey);

    List getAllStreamEntryKeys(boolean checkForExpiry);

    List getAllStreamEntryKeys(boolean checkForExpiry, boolean includeNoReads, boolean includeNoWrites, boolean includeReadsOnly, boolean includeWritesOnly);
}
