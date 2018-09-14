package org.ehcache.extensions.io;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import org.junit.Assert;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Created by FabienSanglier on 5/5/15.
 */
public abstract class EhcacheStreamingTestsBase {
    public static final String ENV_CACHE_NAME = "ehcache.config.cachename";
    public static final String ENV_CACHEMGR_NAME = "ehcache.config.cachemgr.name";
    public static final String ENV_CACHE_CONFIGPATH = "ehcache.config.path";
    public static final String ENV_CACHEKEY_TYPE = "ehcache.tests.cachekeytype";

    public static final String DEFAULT_CACHE_PATH = "classpath:ehcache_localheap.xml";
    public static final String DEFAULT_CACHE_NAME = "FileStore";
    public static final String DEFAULT_CACHEMGR_NAME = "EhcacheStreamsTest";
    public static final String DEFAULT_CACHEKEY_TYPE = "string";

    protected static final int IN_FILE_SIZE = 10 * 1024 * 1024;
    protected static final Path TESTS_DIR_PATH = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"));
    protected static final Path IN_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(),"sample_big_file_in.txt");
    protected static final Path OUT_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), "sample_big_file_out.txt");
    public static NumberFormat formatD = new DecimalFormat("#.###");

    private static CacheManager cm;
    private Ehcache cache;

    private static final String CACHEKEY_TYPE_STRING = "somekey";
    private static final Object CACHEKEY_TYPE_CUSTOMOBJECT = CustomPublicKey.generateRandom();

    public class EhcacheInputStreamParams{
        public final Ehcache cache;
        public final Object cacheKey;
        public final boolean allowNullStream;
        public final int bufferSize;
        public final long openTimeout;

        public EhcacheInputStreamParams(Ehcache cache, Object cacheKey, boolean allowNullStream, int bufferSize, long openTimeout) {
            this.cache = cache;
            this.cacheKey = cacheKey;
            this.allowNullStream = allowNullStream;
            this.bufferSize = bufferSize;
            this.openTimeout = openTimeout;
        }
    }

    public class EhcacheOuputStreamParams{
        public final Ehcache cache;
        public final Object cacheKey;
        public final boolean override;
        public final int bufferSize;
        public final long openTimeout;

        public EhcacheOuputStreamParams(Ehcache cache, Object cacheKey, boolean override, int bufferSize, long openTimeout) {
            this.cache = cache;
            this.cacheKey = cacheKey;
            this.override = override;
            this.bufferSize = bufferSize;
            this.openTimeout = openTimeout;
        }
    }

    private static class CustomPublicKey implements Serializable {
        private String att1;
        private Long att2;
        private Integer att3;
        private Float att4;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CustomPublicKey that = (CustomPublicKey) o;

            if (att1 != null ? !att1.equals(that.att1) : that.att1 != null) return false;
            if (att2 != null ? !att2.equals(that.att2) : that.att2 != null) return false;
            if (att3 != null ? !att3.equals(that.att3) : that.att3 != null) return false;
            if (att4 != null ? !att4.equals(that.att4) : that.att4 != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = att1 != null ? att1.hashCode() : 0;
            result = 31 * result + (att2 != null ? att2.hashCode() : 0);
            result = 31 * result + (att3 != null ? att3.hashCode() : 0);
            result = 31 * result + (att4 != null ? att4.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CustomPublicKey{" +
                    "att1='" + att1 + '\'' +
                    ", att2=" + att2 +
                    ", att3=" + att3 +
                    ", att4=" + att4 +
                    '}';
        }

        public static CustomPublicKey generateRandom(){
            Random rnd = new Random(System.currentTimeMillis());
            CustomPublicKey customPublicKey = new CustomPublicKey();
            customPublicKey.att1 = generateRandomText(rnd,100);
            customPublicKey.att2 = rnd.nextLong();
            customPublicKey.att3 = rnd.nextInt();
            customPublicKey.att4 = rnd.nextFloat();
            return customPublicKey;
        }

        private static String generateRandomText(Random rnd, int StringLength) {
            if (StringLength == 0) {
                return null;
            }
            StringBuffer returnVal = new StringBuffer();
            String[] vals = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
            for (int lp = 0; lp < StringLength; lp++) {
                returnVal.append(vals[rnd.nextInt(vals.length)]);
            }
            return returnVal.toString();
        }
    }

    public Object getCacheKey(){
        String valStr = System.getProperty(ENV_CACHEKEY_TYPE, DEFAULT_CACHEKEY_TYPE);
        if("string".equalsIgnoreCase(valStr))
            return CACHEKEY_TYPE_STRING;
        else if ("object".equalsIgnoreCase(valStr)) {
            return CACHEKEY_TYPE_CUSTOMOBJECT;
        } else {
            throw new IllegalArgumentException("CacheKey Type " + ((null != valStr)?valStr.toString():"null") + " not valid");
        }
    }

    public Ehcache getCache(){
        return cache;
    }

    public static void cacheStart() throws Exception {
        System.out.println("============ cacheStart ====================");
        cm = getCacheManager(System.getProperty(ENV_CACHEMGR_NAME, DEFAULT_CACHEMGR_NAME), System.getProperty(ENV_CACHE_CONFIGPATH, DEFAULT_CACHE_PATH));
    }

    public static void cacheShutdown() throws Exception {
        System.out.println("============ cacheShutdown ====================");
        if(null != cm)
            cm.shutdown();

        cm = null;
    }

    public void cacheSetUp() throws Exception {
        System.out.println("============ cacheSetUp ====================");

        String cacheName = System.getProperty(ENV_CACHE_NAME, DEFAULT_CACHE_NAME);
        try {
            cache = cm.getCache(cacheName);
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (ClassCastException e) {
            e.printStackTrace();
        } catch (CacheException e) {
            e.printStackTrace();
        }

        if (cache == null) {
            throw new IllegalArgumentException("Could not find the cache " + cacheName);
        }

        //empty the cache
        cache.removeAll();
    }

    public void cacheCleanUp(){
        System.out.println("============ cacheCleanUp ====================");
        if(null != cache)
            cache.removeAll();
        cache = null;
    }

    public static long generateBigInputFile() throws IOException {
        long fileChecksum = 0L;

        try (
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(IN_FILE_PATH)), new CRC32());
        ) {
            System.out.println("============ Generate Initial Big File ====================");
            System.out.println("Path: " + IN_FILE_PATH);

            byte[] buffer = new byte[10 * 1024];
            int len = buffer.length;

            long start = System.nanoTime();;
            int byteCount = 0;
            for (int i = 0; i < IN_FILE_SIZE; i++) {
                if(byteCount == len || i == IN_FILE_SIZE - 1){
                    os.write(buffer, 0, byteCount);
                    byteCount=0;
                }

                buffer[byteCount]=new Integer(i).byteValue();
                byteCount++;
            }
            long end = System.nanoTime();

            fileChecksum = os.getChecksum().getValue();

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("CheckSum = " + fileChecksum);
            System.out.println("============================================");
        }

        return fileChecksum;
    }

    public static void cleanBigInputFile() throws IOException {
        if(Files.exists(IN_FILE_PATH))
            Files.delete(IN_FILE_PATH);
    }

    public static void cleanBigOutputFile() throws IOException {
        if(Files.exists(OUT_FILE_PATH))
            Files.delete(OUT_FILE_PATH);
    }

    public long copyFileToCache(final Object publicCacheKey) throws Exception {
        int inBufferSize = 32 * 1024;
        int copyBufferSize = 128 * 1024;
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        System.out.println("============ copyFileToCache ====================");
        System.out.println("Before Cache Size = " + cache.getSize());

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(EhcacheIOStreams.getOutputStream(cache, publicCacheKey),new CRC32())
        )
        {
            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + cache.getSize());
        System.out.println("============================================");

        return outputChecksum;
    }

    public long readFileFromDisk() throws Exception {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH)), new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
        )
        {
            System.out.println("============ readFileFromDisk ====================");

            int copyBufferSize = 32*1024;

            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println(String.format("CheckSums Input: %d // Output = %d", inputChecksum, outputChecksum));
            System.out.println("============================================");
        }

        return outputChecksum;
    }

    public long readFileFromCache(final Object publicCacheKey) throws IOException {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 128 * 1024;
        int copyBufferSize = 64 * 1024; //copy buffer size *smaller* than ehcache input stream internal buffer to make sure it works that way
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(cache, publicCacheKey, false, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
        )
        {
            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        Assert.assertEquals(inputChecksum, outputChecksum);

        return outputChecksum;
    }

    private static CacheManager getCacheManager(String cacheManagerName, String resourcePath) {
        CacheManager cm = null;
        if (null == (cm = CacheManager.getCacheManager(cacheManagerName))) {
            String configLocationToLoad = null;
            if (null != resourcePath && !"".equals(resourcePath)) {
                configLocationToLoad = resourcePath;
            } else if (null != System.getProperty(ENV_CACHE_CONFIGPATH)) {
                configLocationToLoad = System.getProperty(ENV_CACHE_CONFIGPATH);
            }

            if (null != configLocationToLoad) {
                InputStream inputStream = null;
                try {
                    if (configLocationToLoad.indexOf("file:") > -1) {
                        inputStream = new FileInputStream(configLocationToLoad.substring("file:".length()));
                    } else if (configLocationToLoad.indexOf("classpath:") > -1) {
                        inputStream = EhcacheStreamingTestsBase.class.getClassLoader().getResourceAsStream(configLocationToLoad.substring("classpath:".length()));
                    } else { //default to classpath if no prefix is specified
                        inputStream = EhcacheStreamingTestsBase.class.getClassLoader().getResourceAsStream(configLocationToLoad);
                    }

                    if (inputStream == null) {
                        throw new FileNotFoundException("File at '" + configLocationToLoad + "' not found");
                    }

                    cm = CacheManager.create(inputStream);
                } catch (IOException ioe) {
                    throw new CacheException(ioe);
                } finally {
                    if (null != inputStream) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            throw new CacheException(e);
                        }
                        inputStream = null;
                    }
                }
            } else {
                cm = CacheManager.getInstance();
            }
        }

        return cm;
    }

    protected void pipeStreamsByteByByte(InputStream is, OutputStream os) throws IOException {
        int n;
        while ((n = is.read()) > -1) {
            os.write(n);
        }
    }

    protected void pipeStreamsWithBuffer(InputStream is, OutputStream os, int bufferSize) throws IOException {
        int n;
        byte[] buffer = new byte[bufferSize];
        while ((n = is.read(buffer)) > -1) {
            os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
        }
    }
}
