package org.ehcache.extensions.io;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.impl.EhcacheInputStream;
import org.ehcache.extensions.io.impl.EhcacheOutputStream;
import org.ehcache.extensions.io.impl.EhcacheStreamUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created by fabien.sanglier on 7/27/18.
 */
public class EhcacheIOStreams {
    public static final Integer inputStreamBufferSize = getPropertyAsInt(EhcacheStreamUtils.PROP_INPUTSTREAM_BUFFERSIZE, EhcacheStreamUtils.DEFAULT_INPUTSTREAM_BUFFER_SIZE);
    public static final Integer outputStreamBufferSize = getPropertyAsInt(EhcacheStreamUtils.PROP_OUTPUTSTREAM_BUFFERSIZE, EhcacheStreamUtils.DEFAULT_OUTPUTSTREAM_BUFFER_SIZE);
    public static final Long inputStreamOpenTimeout = getPropertyAsLong(EhcacheStreamUtils.PROP_INPUTSTREAM_OPEN_TIMEOUTS, EhcacheStreamUtils.DEFAULT_INPUTSTREAM_OPEN_TIMEOUT);
    public static final Long outputStreamOpenTimeout = getPropertyAsLong(EhcacheStreamUtils.PROP_OUTPUTSTREAM_OPEN_TIMEOUTS, EhcacheStreamUtils.DEFAULT_OUTPUTSTREAM_OPEN_TIMEOUT);
    public static final Boolean inputStreamAllowNulls = getPropertyAsBoolean(EhcacheStreamUtils.PROP_INPUTSTREAM_ALLOW_NULLSTREAM, EhcacheStreamUtils.DEFAULT_INPUTSTREAM_ALLOW_NULLSTREAM);
    public static final Boolean outputStreamDefaultOverride = getPropertyAsBoolean(EhcacheStreamUtils.PROP_OUTPUTSTREAM_OVERRIDE, EhcacheStreamUtils.DEFAULT_OUTPUTSTREAM_OVERRIDE);

    private static long getPropertyAsLong(String key, long defaultVal) {
        String valStr = System.getProperty(key, new Long(defaultVal).toString());
        long val;
        try {
            val = Long.parseLong(valStr);
        } catch (NumberFormatException nfe) {
            val = defaultVal;
        }
        return val;
    }

    private static int getPropertyAsInt(String key, int defaultVal) {
        String valStr = System.getProperty(key, new Integer(defaultVal).toString());
        int val;
        try {
            val = Integer.parseInt(valStr);
        } catch (NumberFormatException nfe) {
            val = defaultVal;
        }
        return val;
    }

    private static boolean getPropertyAsBoolean(String key, boolean defaultVal) {
        String valStr = System.getProperty(key, new Boolean(defaultVal).toString());
        return Boolean.parseBoolean(valStr);
    }

    //////////////////////////// Public Utils

    /**
     * Check if a Stream entry exist in cache
     *
     * @param      cache  the backend cache
     * @param      cacheKey  the public cache key for this stream entry
     * @return     true if Stream entry is in cache
     * @exception  EhcacheStreamException if cache or cacheKey are not valid
     */
    public static boolean containStreamEntry(Ehcache cache, Object cacheKey) throws EhcacheStreamException {
        checkValid(cache, cacheKey);

        return new EhcacheStreamUtils(cache).containsStreamEntry(cacheKey);
    }

    /**
     * Get a list of all the public keys (the key objects used by the client apps) in cache
     *
     * @return      List of public key objects
     */

    /**
     * Get list of all the cache entry keys
     *
     * @param      cache  the backend cache
     * @return     List of stream entry keys in cache
     * @exception  EhcacheStreamException if cache or cacheKey are not valid
     */
    public static List getStreamEntryKeys(Ehcache cache, boolean excludeExpiredKeys) throws EhcacheStreamException {
        checkValid(cache);

        return new EhcacheStreamUtils(cache).getAllStreamEntryKeys(excludeExpiredKeys);
    }

    /**
     * Remove a stream entry from cache
     *
     * @param       cache           the underlying cache to access
     * @param       cacheKey        the underlying cache key to read data from
     * @return      true if the value was removed
     * @exception   EhcacheStreamException if cache is null, disabled, or cacheKey is null, OR if the remove operation was not successful
     */
    public static boolean removeStreamEntry(Ehcache cache, Object cacheKey) throws EhcacheStreamException {
        return removeStreamEntry(cache, cacheKey, outputStreamOpenTimeout);
    }

    /**
     * Remove a stream entry from cache
     *
     * @param       cache           the underlying cache to access
     * @param       cacheKey        the underlying cache key to read data from
     * @param       openTimeout     the timeout on the stream open operation
     * @return      true if the value was removed
     * @exception   EhcacheStreamException if cache is null, disabled, or cacheKey is null, OR if the remove operation was not successful
     */
    public static boolean removeStreamEntry(Ehcache cache, Object cacheKey, long openTimeout) throws EhcacheStreamException {
        checkValid(cache, cacheKey);

        return new EhcacheStreamUtils(cache).removeStreamEntry(cacheKey, openTimeout);
    }

    //////////////////////////// InputStream

    public static InputStream getInputStream(Ehcache cache, Object cacheKey) throws EhcacheStreamException {
        return getInputStream(cache, cacheKey, inputStreamAllowNulls);
    }

    public static InputStream getInputStream(Ehcache cache, Object cacheKey, boolean allowNullStream) throws EhcacheStreamException {
        return getInputStream(cache, cacheKey, allowNullStream, inputStreamBufferSize);
    }

    public static InputStream getInputStream(Ehcache cache, Object cacheKey, boolean allowNullStream, int bufferSize) throws EhcacheStreamException {
        return getInputStream(cache, cacheKey, allowNullStream, bufferSize, inputStreamOpenTimeout);
    }

    /**
     * Get an InputStream object backed by Ehcache.
     *
     * @param       cache           the underlying cache to access
     * @param       cacheKey        the underlying cache key to read data from
     * @param       allowNullStream flag to specify if this method should return NULL when the cache does not contain the underlying cache key to read data from.
     * @param       bufferSize      the outputStream underlying buffer size. This essentially specify a max size for each of the underlying cache "chunk entries" created in Ehcache.
     * @param       openTimeout     the timeout for the stream exclusive open operation (write lock timeout)
     * @return      a valid InputStream object
     * @exception   EhcacheStreamException if cache is null, disabled, or cacheKey is null, OR if the EhcacheInputStream creation was not successful
     */
    public static InputStream getInputStream(Ehcache cache, Object cacheKey, boolean allowNullStream, int bufferSize, long openTimeout) throws EhcacheStreamException {
        checkValid(cache, cacheKey);

        if(!allowNullStream || allowNullStream && containStreamEntry(cache, cacheKey)){
            return new EhcacheInputStream(
                    cache,
                    cacheKey,
                    bufferSize,
                    openTimeout);
        } else {
            return null;
        }
    }

    //////////////////////////// OutputStream

    public static OutputStream getOutputStream(Ehcache cache, Object cacheKey) throws EhcacheStreamException {
        return getOutputStream(cache, cacheKey, outputStreamDefaultOverride);
    }

    public static OutputStream getOutputStream(Ehcache cache, Object cacheKey, boolean override) throws EhcacheStreamException {
        return getOutputStream(cache, cacheKey, override, outputStreamBufferSize);
    }

    public static OutputStream getOutputStream(Ehcache cache, Object cacheKey, boolean override, int bufferSize) throws EhcacheStreamException {
        return getOutputStream(cache, cacheKey, override, bufferSize, outputStreamOpenTimeout);
    }

    /**
     * Get an OutputStream object backed by Ehcache.
     *
     * @param       cache           the underlying cache to access
     * @param       cacheKey        the underlying cache key to read data from
     * @param       override        flag to specify if the new data should completely override the currently stored data, or if it should append to the existing stored data.
     * @param       bufferSize      the outputStream underlying buffer size. This essentially specify a max size for each of the underlying cache "chunk entries" created in Ehcache.
     * @param       openTimeout     the timeout for the stream exclusive open operation (write lock timeout)
     * @return      a Valid OutputStream object
     * @exception   EhcacheStreamException if cache is null, disabled, or cacheKey is null, OR if the EhcacheOutputStream creation was not successful
     */
    public static OutputStream getOutputStream(Ehcache cache, Object cacheKey, boolean override, int bufferSize, long openTimeout) throws EhcacheStreamException {
        checkValid(cache, cacheKey);

        EhcacheOutputStream ehcacheStream = new EhcacheOutputStream(
                cache,
                cacheKey,
                bufferSize,
                override,
                openTimeout
        );

        return ehcacheStream;
    }

    //////////////////////////// Internal Validators

    private static void checkValid(Ehcache cache) throws EhcacheStreamException {
        if(cache == null)
            throw new EhcacheStreamException("Cache may not be null");

        if(cache.isDisabled())
            throw new EhcacheStreamException("Cache is disabled");

    }

    private static void checkValid(Ehcache cache, Object cacheKey) throws EhcacheStreamException {
        checkValid(cache);

        if(cacheKey == null)
            throw new EhcacheStreamException("cacheKey may not be null");
    }
}
