package org.ehcache.extensions.io;

import net.sf.ehcache.Cache;
import org.ehcache.extensions.io.impl.EhcacheInputStream;
import org.ehcache.extensions.io.impl.EhcacheOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by fabien.sanglier on 7/27/18.
 */
public class EhcacheStreamFactory {
    public static String PROP_INPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.inputstream.buffersize";
    public static String PROP_OUTPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.outputstream.buffersize";
    private static boolean ALLOW_NULL_STREAM_DEFAULT = false;

    private static final Integer inputStreamBufferSize;
    private static final Integer ouputStreamBufferSize;

    static {
        String inputStreamBufferSizeStr = System.getProperty(PROP_INPUTSTREAM_BUFFERSIZE);
        String ouputStreamBufferSizeStr = System.getProperty(PROP_OUTPUTSTREAM_BUFFERSIZE);

        int inputStreamBufferSizeInternal = 0;
        if(null != inputStreamBufferSizeStr && !"".equals(inputStreamBufferSizeStr)) {
            try {
                inputStreamBufferSizeInternal = Integer.parseInt(inputStreamBufferSizeStr);
            } catch (NumberFormatException nfe) {
                inputStreamBufferSizeInternal = 0;
            }
        }

        int outputStreamBufferSizeInternal = 0;
        if(null != ouputStreamBufferSizeStr && !"".equals(ouputStreamBufferSizeStr)) {
            try {
                outputStreamBufferSizeInternal = Integer.parseInt(ouputStreamBufferSizeStr);
            } catch (NumberFormatException nfe) {
                outputStreamBufferSizeInternal = 0;
            }
        }

        inputStreamBufferSize = (inputStreamBufferSizeInternal > 0)?new Integer(inputStreamBufferSizeInternal):null;
        ouputStreamBufferSize = (outputStreamBufferSizeInternal > 0)?new Integer(outputStreamBufferSizeInternal):null;
    }

    private static boolean checkInputStreamNotEmpty(EhcacheInputStream stream) throws EhcacheStreamException {
        int streamSize = 0;
        try {
            streamSize = stream.available();
        } catch (IOException e) {
            throw new EhcacheStreamException("error while checking the input stream available bytes", e);
        }

        return streamSize > 0;
    }

    private static boolean checkCacheValid(Cache cache) throws EhcacheStreamException {
        if(cache == null)
            throw new EhcacheStreamException("Cache may not be null");
        return !cache.isDisabled();
    }

    //////////////////////////// InputStream

    public static InputStream getInputStream(Cache cache, Object cacheKey) throws EhcacheStreamException {
        return getInputStream(cache, cacheKey, ALLOW_NULL_STREAM_DEFAULT);
    }

    public static InputStream getInputStream(Cache cache, Object cacheKey, boolean allowNullStream) throws EhcacheStreamException {
        if(!checkCacheValid(cache))
            return null;

        EhcacheInputStream ehcacheStream = null;
        if(null != inputStreamBufferSize)
            ehcacheStream = new EhcacheInputStream(cache, cacheKey, inputStreamBufferSize);
        else
            ehcacheStream = new EhcacheInputStream(cache, cacheKey);

        return (!allowNullStream || allowNullStream && checkInputStreamNotEmpty(ehcacheStream))?ehcacheStream: null;
    }

    public static InputStream getInputStream(Cache cache, Object cacheKey, int bufferSize) throws EhcacheStreamException {
        return getInputStream(cache, cacheKey, bufferSize, ALLOW_NULL_STREAM_DEFAULT);
    }

    public static InputStream getInputStream(Cache cache, Object cacheKey, int bufferSize, boolean allowNullStream) throws EhcacheStreamException {
        if(!checkCacheValid(cache))
            return null;

        EhcacheInputStream ehcacheStream = new EhcacheInputStream(cache, cacheKey, bufferSize);
        return (!allowNullStream || allowNullStream && checkInputStreamNotEmpty(ehcacheStream))?ehcacheStream: null;
    }

    //////////////////////////// OutputStream

    public static OutputStream getOutputStream(Cache cache, Object cacheKey) throws EhcacheStreamException {
        return getOutputStream(cache, cacheKey, EhcacheOutputStream.OVERRIDE_DEFAULT);
    }

    public static OutputStream getOutputStream(Cache cache, Object cacheKey, int bufferSize) throws EhcacheStreamException {
        return getOutputStream(cache, cacheKey, bufferSize, EhcacheOutputStream.OVERRIDE_DEFAULT);
    }

    public static OutputStream getOutputStream(Cache cache, Object cacheKey, int bufferSize, boolean override) throws EhcacheStreamException {
        if(!checkCacheValid(cache))
            return null;

        EhcacheOutputStream ehcacheStream = new EhcacheOutputStream(cache, cacheKey, bufferSize, override);
        return ehcacheStream;
    }

    public static OutputStream getOutputStream(Cache cache, Object cacheKey, boolean override) throws EhcacheStreamException {
        if(!checkCacheValid(cache))
            return null;

        EhcacheOutputStream ehcacheStream = null;
        if(null != ouputStreamBufferSize)
            ehcacheStream = new EhcacheOutputStream(cache, cacheKey, ouputStreamBufferSize, override);
        else
            ehcacheStream = new EhcacheOutputStream(cache, cacheKey, override);

        return ehcacheStream;
    }
}
