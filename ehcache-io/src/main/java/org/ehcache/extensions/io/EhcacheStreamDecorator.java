package org.ehcache.extensions.io;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.constructs.EhcacheDecoratorAdapter;
import net.sf.ehcache.loader.CacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by fabien.sanglier on 9/17/18.
 */
public class EhcacheStreamDecorator extends EhcacheDecoratorAdapter {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamDecorator.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();
    public static final NumberFormat formatD = new DecimalFormat("#.###");

    private final boolean useCompression;
    private final boolean useOverwriteOnPuts;
    private final int bufferSize;
    private final boolean returnAsStreamForGets;
    private final OutputStream outputStreamForGets; // TODO: this behavior of returning a stream on gets won't work well without more work...
    private final boolean allowNullStreamOnGets = false;

    public EhcacheStreamDecorator(Ehcache underlyingCache, final int bufferSize, final boolean useCompression, final boolean useOverwriteOnPuts, final boolean returnAsStreamForGets, final OutputStream outputStreamForGets) {
        super(underlyingCache);
        this.bufferSize = bufferSize;
        this.useCompression = useCompression;
        this.useOverwriteOnPuts = useOverwriteOnPuts;
        this.returnAsStreamForGets = returnAsStreamForGets;
        this.outputStreamForGets = outputStreamForGets;
    }

    public void streamToCache(Object cacheKey, Object cacheValue) throws IOException {
        if(cacheValue instanceof InputStream) {
            try (
                    InputStream is = new BufferedInputStream((InputStream) cacheValue, bufferSize);
                    OutputStream os =
                            (useCompression) ? new GZIPOutputStream(EhcacheIOStreams.getOutputStream(underlyingCache, cacheKey, useOverwriteOnPuts, bufferSize)) :
                                    EhcacheIOStreams.getOutputStream(underlyingCache, cacheKey, useOverwriteOnPuts, bufferSize);
            ) {
                if (isDebug)
                    logger.debug("============ coping Stream to cache " + ((useCompression) ? "(with Gzip Compression)" : "") + " ====================");

                long start = System.nanoTime();
                pipeStreamsWithBuffer(is, os, bufferSize);
                long end = System.nanoTime();

                if (isTrace)
                    logger.trace("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
            }
        }
        else {
            try (
                    ObjectOutputStream os =
                            new ObjectOutputStream(
                                    (useCompression) ? new GZIPOutputStream(EhcacheIOStreams.getOutputStream(underlyingCache, cacheKey, useOverwriteOnPuts, bufferSize)) :
                                            EhcacheIOStreams.getOutputStream(underlyingCache, cacheKey, useOverwriteOnPuts, bufferSize)
                            );
            ) {
                os.writeObject(cacheValue);
            }
        }
    }

    // TODO: this behavior of returning a stream on gets won't work well without more work...
    public Object streamFromCache(Object cacheKey) throws IOException, ClassNotFoundException {
        Object fromCache = null;
        if(returnAsStreamForGets){
            if(null == outputStreamForGets){
                try (
                        InputStream is =
                                (useCompression) ? new GZIPInputStream(EhcacheIOStreams.getInputStream(underlyingCache, cacheKey, allowNullStreamOnGets, bufferSize)) :
                                        EhcacheIOStreams.getInputStream(underlyingCache, cacheKey, allowNullStreamOnGets, bufferSize);
                        OutputStream os = new ByteArrayOutputStream();
                ) {
                    if (isDebug)
                        logger.debug("============ coping cache entry back to stream " + ((useCompression) ? "(with Gzip Compression)" : "") + " ====================");

                    long start = System.nanoTime();
                    pipeStreamsWithBuffer(is, os, bufferSize);
                    long end = System.nanoTime();

                    if (isTrace)
                        logger.trace("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");

                    os.flush();
                    fromCache = ((ByteArrayOutputStream)os).toByteArray();
                }
            } else {
                try (
                        InputStream is =
                                (useCompression) ? new GZIPInputStream(EhcacheIOStreams.getInputStream(underlyingCache, cacheKey, allowNullStreamOnGets, bufferSize)) :
                                        EhcacheIOStreams.getInputStream(underlyingCache, cacheKey, allowNullStreamOnGets, bufferSize);
                ) {
                    if (isDebug)
                        logger.debug("============ coping cache entry back to stream " + ((useCompression) ? "(with Gzip Compression)" : "") + " ====================");

                    long start = System.nanoTime();
                    ;
                    pipeStreamsWithBuffer(is, outputStreamForGets, bufferSize);
                    long end = System.nanoTime();
                    ;

                    if (isTrace)
                        logger.trace("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
                }
                fromCache = outputStreamForGets;
            }
        } else {
            try (
                    ObjectInputStream objectInputStream =
                            new ObjectInputStream(
                                    (useCompression) ? new GZIPInputStream(EhcacheIOStreams.getInputStream(underlyingCache, cacheKey, allowNullStreamOnGets, bufferSize)) :
                                            EhcacheIOStreams.getInputStream(underlyingCache, cacheKey, allowNullStreamOnGets, bufferSize)
                            );
            ) {
                fromCache = objectInputStream.readObject();
            }
        }

        return fromCache;
    }

    void pipeStreamsWithBuffer(InputStream is, OutputStream os, int bufferSize) throws IOException {
        int n;
        byte[] buffer = new byte[bufferSize];
        while ((n = is.read(buffer)) > -1) {
            os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
        }
    }

    @Override
    public Element get(Object key) throws IllegalStateException, CacheException {
        Object fromCache = null;
        try {
            fromCache = streamFromCache(key);
        } catch (IOException e) {
            new CacheException(e);
        } catch (ClassNotFoundException e) {
            new IllegalStateException(e);
        }
        if(null != fromCache) return new Element(key,fromCache);
        else return null;
    }

    @Override
    public void put(Element element) throws IllegalArgumentException, IllegalStateException, CacheException {
        if(element == null) throw new IllegalArgumentException("element may not be null");

        try {
            streamToCache(element.getObjectKey(), element.getObjectValue());
        } catch (IOException e) {
            new CacheException(e);
        }
    }

    @Override
    public boolean remove(Object key) throws IllegalStateException {
        try {
            return EhcacheIOStreams.removeStreamEntry(underlyingCache, key);
        } catch (EhcacheStreamException e) {
            new IllegalStateException(e);
        }
        return false;
    }

    @Override
    public Map<Object, Element> getAll(Collection<?> keys) throws IllegalStateException, CacheException {
        Map<Object, Element> keyValues = new HashMap<>();
        for(Object key:keys){
            keyValues.put(key, new Element(key, get(key)));
        }
        return keyValues;
    }

    @Override
    public Element get(Serializable key) throws IllegalStateException, CacheException {
        Object fromCache = null;
        try {
            fromCache = streamFromCache(key);
        } catch (IOException e) {
            new CacheException(e);
        } catch (ClassNotFoundException e) {
            new IllegalStateException(e);
        }
        if(null != fromCache) return new Element(key,fromCache);
        else return null;
    }

    @Override
    public Element getQuiet(Object key) throws IllegalStateException, CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Element getQuiet(Serializable key) throws IllegalStateException, CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(Element element, boolean doNotNotifyCacheReplicators) throws IllegalArgumentException, IllegalStateException, CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Collection<Element> elements) throws IllegalArgumentException, IllegalStateException, CacheException {
        for(Element element:elements){
            put(element);
        }
    }

    @Override
    public void putQuiet(Element element) throws IllegalArgumentException, IllegalStateException, CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putWithWriter(Element element) throws IllegalArgumentException, IllegalStateException, CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, boolean doNotNotifyCacheReplicators) throws IllegalStateException {
        return remove(key);
    }

    @Override
    public void removeAll(Collection<?> keys) throws IllegalStateException {
        for(Object key:keys){
            remove(key);
        }
    }

    @Override
    public void removeAll(Collection<?> keys, boolean doNotNotifyCacheReplicators) throws IllegalStateException {
        removeAll(keys);
    }

    @Override
    public boolean remove(Serializable key, boolean doNotNotifyCacheReplicators) throws IllegalStateException {
        return remove(key);
    }

    @Override
    public boolean remove(Serializable key) throws IllegalStateException {
        return remove(key);
    }

    @Override
    public List getKeys() throws IllegalStateException, CacheException {
        return getKeysWithExpiryCheck();
    }

    @Override
    public List getKeysNoDuplicateCheck() throws IllegalStateException {
        try {
            return EhcacheIOStreams.getStreamEntryKeys(underlyingCache, false);
        } catch (EhcacheStreamException e) {
            throw new CacheException(e);
        }
    }

    @Override
    public List getKeysWithExpiryCheck() throws IllegalStateException, CacheException {
        try {
            return EhcacheIOStreams.getStreamEntryKeys(underlyingCache, true);
        } catch (EhcacheStreamException e) {
            throw new CacheException(e);
        }
    }

    @Override
    public boolean isKeyInCache(Object key) {
        try {
            return EhcacheIOStreams.containStreamEntry(underlyingCache, key);
        } catch (EhcacheStreamException e) {
            new IllegalStateException(e);
        }
        return false;
    }

    @Override
    public boolean isValueInCache(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Element getWithLoader(Object key, CacheLoader loader, Object loaderArgument) throws CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map getAllWithLoader(Collection keys, Object loaderArgument) throws CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void load(Object key) throws CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadAll(Collection keys, Object argument) throws CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeQuiet(Object key) throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeQuiet(Serializable key) throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeWithWriter(Object key) throws IllegalStateException, CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Element putIfAbsent(Element element) throws NullPointerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Element putIfAbsent(Element element, boolean doNotNotifyCacheReplicators) throws NullPointerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeElement(Element element) throws NullPointerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(Element old, Element element) throws NullPointerException, IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Element replace(Element element) throws NullPointerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Element removeAndReturnElement(Object key) throws IllegalStateException {
        throw new UnsupportedOperationException();
    }
}
