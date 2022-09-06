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

/**
 * Created by fabien.sanglier on 9/17/18.
 */
public class EhcacheStreamDecorator extends EhcacheDecoratorAdapter {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamDecorator.class);
    private static final boolean isDebug = logger.isDebugEnabled();
    public static final NumberFormat formatD = new DecimalFormat("#.###");

    private final boolean useOverwriteOnPuts;
    private final int bufferSizeOnPuts;

    private final int bufferSizeOnGets;
    private final boolean returnAsBytesOnGets;
    private final boolean allowNullStreamOnGets = false;

    public EhcacheStreamDecorator(Ehcache underlyingCache, boolean useOverwriteOnPuts, int bufferSizeOnPuts, int bufferSizeOnGets, boolean returnAsBytesOnGets) {
        super(underlyingCache);
        this.useOverwriteOnPuts = useOverwriteOnPuts;
        this.bufferSizeOnPuts = bufferSizeOnPuts;
        this.bufferSizeOnGets = bufferSizeOnGets;
        this.returnAsBytesOnGets = returnAsBytesOnGets;
    }

    public Ehcache getUnderlyingCache(){
        return underlyingCache;
    }

    public void streamToCache(Object cacheKey, Object cacheValue) throws IOException {
        if(cacheValue instanceof InputStream || cacheValue instanceof byte[]) {
            InputStream is = null;
            OutputStream os = null;
            try {
                if (cacheValue instanceof InputStream) {
                    is = new BufferedInputStream((InputStream) cacheValue, bufferSizeOnPuts);
                } else if (cacheValue instanceof byte[]) {
                    is = new ByteArrayInputStream((byte[]) cacheValue);
                }

                os = EhcacheIOStreams.getOutputStream(underlyingCache, cacheKey, useOverwriteOnPuts, bufferSizeOnPuts);

                if (isDebug)
                    logger.debug("============ copying Stream to cache ====================");

                long start = System.nanoTime();
                pipeStreamsWithBuffer(is, os, bufferSizeOnPuts);
                long end = System.nanoTime();

                if (isDebug)
                    logger.debug("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
            } finally {
                if(null != is)
                    is.close();

                if(null != os)
                    os.close();
            }
        } else {
            ObjectOutputStream os = null;
            try{
                os = new ObjectOutputStream(EhcacheIOStreams.getOutputStream(underlyingCache, cacheKey, useOverwriteOnPuts, bufferSizeOnPuts));
                os.writeObject(cacheValue);
            } finally {
                if(null != os)
                    os.close();
            }
        }
    }

    public Object streamFromCache(Object cacheKey) throws IOException, ClassNotFoundException {
        Object fromCache = null;
        if(returnAsBytesOnGets){
            InputStream is = null;
            OutputStream os = null;
            try{
                is = EhcacheIOStreams.getInputStream(underlyingCache, cacheKey, allowNullStreamOnGets, bufferSizeOnGets);
                os = new ByteArrayOutputStream();

                if (isDebug)
                    logger.debug("============ coping cache entry back to stream ====================");

                long start = System.nanoTime();
                pipeStreamsWithBuffer(is, os, bufferSizeOnGets);
                long end = System.nanoTime();

                if (isDebug)
                    logger.debug("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");

                os.flush();
                fromCache = ((ByteArrayOutputStream)os).toByteArray();
            } finally {
                if(null != is)
                    is.close();

                if(null != os)
                    os.close();
            }
        } else {
            fromCache = EhcacheIOStreams.getInputStream(underlyingCache, cacheKey, allowNullStreamOnGets, bufferSizeOnGets);
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
        return EhcacheIOStreams.getStreamEntryKeys(underlyingCache, false);
    }

    @Override
    public List getKeysWithExpiryCheck() throws IllegalStateException, CacheException {
        return EhcacheIOStreams.getStreamEntryKeys(underlyingCache, true);
    }

    @Override
    public boolean isKeyInCache(Object key) {
        return EhcacheIOStreams.containStreamEntry(underlyingCache, key);
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
