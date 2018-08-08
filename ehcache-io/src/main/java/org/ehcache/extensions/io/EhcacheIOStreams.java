package org.ehcache.extensions.io;

import net.sf.ehcache.Cache;
import org.ehcache.extensions.io.impl.EhcacheInputStream;
import org.ehcache.extensions.io.impl.EhcacheOutputStream;
import org.ehcache.extensions.io.impl.EhcacheStreamUtils;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by fabien.sanglier on 7/27/18.
 */
public class EhcacheIOStreams {
    public static String PROP_INPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.inputstream.buffersize";
    public static String PROP_OUTPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.outputstream.buffersize";
    public static String PROP_OUTPUTSTREAM_OVERRIDE = "ehcache.extension.io.outputstream.override";
    public static String PROP_OPEN_TIMEOUTS = "ehcache.extension.io.streams.opentimeout";
    public static String PROP_ALLOW_NULLSTREAM = "ehcache.extension.io.streams.allownull";

    private static int DEFAULT_OUTPUTSTREAM_BUFFER_SIZE = 1 * 1024 * 1024; // 1MB
    private static boolean DEFAULT_OUTPUTSTREAM_OVERRIDE = true;
    private static int DEFAULT_INPUTSTREAM_BUFFER_SIZE = 512 * 1024; // 512kb
    private static final long DEFAULT_OPEN_TIMEOUT = 10000;
    private static boolean DEFAULT_ALLOW_NULL_STREAM = false;

    private static final Integer inputStreamBufferSize = getPropertyAsInt(PROP_INPUTSTREAM_BUFFERSIZE, DEFAULT_INPUTSTREAM_BUFFER_SIZE);
    private static final Integer outputStreamBufferSize = getPropertyAsInt(PROP_OUTPUTSTREAM_BUFFERSIZE, DEFAULT_OUTPUTSTREAM_BUFFER_SIZE);
    private static final Long streamOpenTimeouts = getPropertyAsLong(PROP_OPEN_TIMEOUTS, DEFAULT_OPEN_TIMEOUT);
    private static final Boolean streamAllowNulls = getPropertyAsBoolean(PROP_ALLOW_NULLSTREAM, DEFAULT_ALLOW_NULL_STREAM);
    private static final Boolean outputStreamDefaultOverride = getPropertyAsBoolean(PROP_OUTPUTSTREAM_OVERRIDE, DEFAULT_OUTPUTSTREAM_OVERRIDE);

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
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param       cache           the underlying cache to access
     * @param       cacheKey        the underlying cache key to read data from
     * @return      true if entry exists
     * @exception   EhcacheStreamException if cache is null, disabled, or cacheKey is null.
     */
    public static boolean checkStreamEntryExist(Cache cache, Object cacheKey) throws EhcacheStreamException {
        checkValid(cache, cacheKey);

        return null != new EhcacheStreamUtils(cache,cacheKey).getStreamMasterFromCache();
    }

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param       cache           the underlying cache to access
     * @param       cacheKey        the underlying cache key to read data from
     * @return      true if the value was removed
     * @exception   EhcacheStreamException if cache is null, disabled, or cacheKey is null, OR if the remove operation was not successful
     */
    public static boolean removeStreamEntry(Cache cache, Object cacheKey) throws EhcacheStreamException {
        return removeStreamEntry(cache, cacheKey, streamOpenTimeouts);
    }

    /**
     * Creates a new buffered output stream to write data to a cache
     * with the specified buffer size.
     *
     * @param       cache           the underlying cache to access
     * @param       cacheKey        the underlying cache key to read data from
     * @param       openTimeout     the timeout for the stream reader open
     * @return      true if the value was removed
     * @exception   EhcacheStreamException if cache is null, disabled, or cacheKey is null, OR if the remove operation was not successful
     */
    public static boolean removeStreamEntry(Cache cache, Object cacheKey, long openTimeout) throws EhcacheStreamException {
        checkValid(cache, cacheKey);

        return new EhcacheStreamUtils(cache,cacheKey).removeEhcacheStreamEntry(openTimeout);
    }

    //////////////////////////// InputStream

    public static InputStream getInputStream(Cache cache, Object cacheKey) throws EhcacheStreamException {
        return getInputStream(cache, cacheKey, streamAllowNulls);
    }

    public static InputStream getInputStream(Cache cache, Object cacheKey, boolean allowNullStream) throws EhcacheStreamException {
        return getInputStream(cache, cacheKey, allowNullStream, inputStreamBufferSize);
    }

    public static InputStream getInputStream(Cache cache, Object cacheKey, boolean allowNullStream, int bufferSize) throws EhcacheStreamException {
        return getInputStream(cache, cacheKey, allowNullStream, bufferSize, streamOpenTimeouts);
    }

    public static InputStream getInputStream(Cache cache, Object cacheKey, boolean allowNullStream, int bufferSize, long openTimeout) throws EhcacheStreamException {
        checkValid(cache, cacheKey);

        if(!allowNullStream || allowNullStream && checkStreamEntryExist(cache,cacheKey)){
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

    public static OutputStream getOutputStream(Cache cache, Object cacheKey) throws EhcacheStreamException {
        return getOutputStream(cache, cacheKey, outputStreamDefaultOverride);
    }

    public static OutputStream getOutputStream(Cache cache, Object cacheKey, boolean override) throws EhcacheStreamException {
        return getOutputStream(cache, cacheKey, override, outputStreamBufferSize);
    }

    public static OutputStream getOutputStream(Cache cache, Object cacheKey, boolean override, int bufferSize) throws EhcacheStreamException {
        return getOutputStream(cache, cacheKey, override, bufferSize, streamOpenTimeouts);
    }

    public static OutputStream getOutputStream(Cache cache, Object cacheKey, boolean override, int bufferSize, long openTimeout) throws EhcacheStreamException {
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

    private static void checkValid(Cache cache, Object cacheKey) throws EhcacheStreamException {
        if(cache == null)
            throw new EhcacheStreamException("Cache may not be null");

        if(cache.isDisabled())
            throw new EhcacheStreamException("Cache is disabled");

        if(cacheKey == null)
            throw new EhcacheStreamException("cacheKey may not be null");
    }
}
