package org.ehcache.extensions.io;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.junit.Assert;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Created by FabienSanglier on 5/5/15.
 */
public abstract class EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamingTestsBase.class);

    public static final String ENV_CACHETEST_TYPE = "ehcache.tests.cachetesttype";
    public static final String ENV_CACHEKEY_TYPE = "ehcache.tests.cachekeytype";

    public static final CacheKeyType DEFAULT_CACHEKEY_TYPE = CacheKeyType.COMPLEX_OBJECT;
    public static final CacheTestType DEFAULT_CACHETEST_TYPE = CacheTestType.CLUSTERED_EVENTUAL;

    protected static final int IN_FILE_SIZE = 10 * 1024 * 1024;
    protected static final Path TESTS_DIR_PATH = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"));
    protected static final Path IN_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(),"sample_big_file_in.txt");
    protected static final Path OUT_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), "sample_big_file_out.txt");
    public static NumberFormat formatD = new DecimalFormat("#.###");

    private static CacheManager cm;
    private Ehcache cache;

    //saved statically so it's the same throughout the tests
    private static final String CACHEKEY_TYPE_STRING = "somekey";
    private static final Object CACHEKEY_TYPE_CUSTOMOBJECT = CustomPublicKey.generateRandom();

    @Parameterized.Parameter(0)
    public PropertyUtils.ConcurrencyMode concurrencyMode = PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS;

    @Parameterized.Parameter(1)
    public CacheKeyType cacheKeyType = CacheKeyType.COMPLEX_OBJECT;

    @Parameterized.Parameters
    public static Collection params() {
        return Arrays.asList(new Object[][]{
                {PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS, CacheKeyType.COMPLEX_OBJECT},
                {PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS, CacheKeyType.STRING},
                {PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS, CacheKeyType.COMPLEX_OBJECT},
                {PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS, CacheKeyType.STRING},
                {PropertyUtils.ConcurrencyMode.WRITE_PRIORITY, CacheKeyType.COMPLEX_OBJECT},
                {PropertyUtils.ConcurrencyMode.WRITE_PRIORITY, CacheKeyType.STRING}
        });
    }

    public void setupParameterizedProperties() {
        logger.info("============ setupParameterizedProperties ====================");

        logger.info("Setting up concurrency mode = {}", concurrencyMode.getPropValue());
        logger.info("Setting up cacheKey type = {}", cacheKeyType.getPropValue());

        System.setProperty(PropertyUtils.PROP_CONCURRENCY_MODE, concurrencyMode.getPropValue());
        System.setProperty(ENV_CACHEKEY_TYPE, cacheKeyType.getPropValue());
    }

    public void cleanupParameterizedProperties() {
        logger.info("============ cleanupParameterizedProperties ====================");

        System.clearProperty(PropertyUtils.PROP_CONCURRENCY_MODE);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE);
    }

    public static enum CacheKeyType {
        STRING("string") {
            @Override
            public Object getCacheKey() {
                return CACHEKEY_TYPE_STRING;
            }
        }, COMPLEX_OBJECT("complex") {
            @Override
            public Object getCacheKey() {
                return CACHEKEY_TYPE_CUSTOMOBJECT;
            }
        };

        public abstract Object getCacheKey();

        private final String propValue;

        CacheKeyType(String propValue) {
            this.propValue = propValue;
        }

        public String getPropValue() {
            return propValue;
        }

        public static CacheKeyType valueOfIgnoreCase(String cacheKeyTypeStr){
            if(null != cacheKeyTypeStr && !"".equals(cacheKeyTypeStr)) {
                if (STRING.propValue.equalsIgnoreCase(cacheKeyTypeStr))
                    return STRING;
                else if (COMPLEX_OBJECT.propValue.equalsIgnoreCase(cacheKeyTypeStr))
                    return COMPLEX_OBJECT;
                else
                    throw new IllegalArgumentException("CacheKeyType [" + ((null != cacheKeyTypeStr) ? cacheKeyTypeStr : "null") + "] is not valid");
            } else {
                return DEFAULT_CACHEKEY_TYPE;
            }
        }
    }

    public enum CacheTestType {
        LOCAL_HEAP("localheap") {
            public String getCacheConfigPath(){
                return "classpath:ehcache_localheap.xml";
            }

            public String getCacheManagerName(){
                return "EhcacheStreamsTest";
            }

            public String getCacheName(){
                return "FileStore";
            }
        },
        LOCAL_OFFHEAP("localoffheap") {
            public String getCacheConfigPath(){
                return "classpath:ehcache_localoffheap.xml";
            }

            public String getCacheManagerName(){
                return "EhcacheStreamsOffheapTest";
            }

            public String getCacheName(){
                return "FileStoreOffheap";
            }
        },
        CLUSTERED_STRONG("clustered_strong") {
            public String getCacheConfigPath(){
                return "classpath:ehcache_distributed.xml";
            }

            public String getCacheManagerName(){
                return "EhcacheStreamsDistributedTest";
            }

            public String getCacheName(){
                return "FileStoreDistributedStrong";
            }
        },
        CLUSTERED_EVENTUAL("clustered_eventual") {
            public String getCacheConfigPath(){
                return "classpath:ehcache_distributed.xml";
            }

            public String getCacheManagerName(){
                return "EhcacheStreamsDistributedTest";
            }

            public String getCacheName(){
                return "FileStoreDistributedEventual";
            }
        };

        private final String propValue;

        CacheTestType(String propValue) {
            this.propValue = propValue;
        }

        public abstract String getCacheConfigPath();
        public abstract String getCacheManagerName();
        public abstract String getCacheName();

        public String getPropValue() {
            return propValue;
        }

        public static CacheTestType valueOfIgnoreCase(String cacheTestTypeStr){
            if(null != cacheTestTypeStr && !"".equals(cacheTestTypeStr)) {
                if (LOCAL_HEAP.propValue.equalsIgnoreCase(cacheTestTypeStr))
                    return LOCAL_HEAP;
                else if (LOCAL_OFFHEAP.propValue.equalsIgnoreCase(cacheTestTypeStr))
                    return LOCAL_OFFHEAP;
                else if (CLUSTERED_EVENTUAL.propValue.equalsIgnoreCase(cacheTestTypeStr))
                    return CLUSTERED_EVENTUAL;
                else if (CLUSTERED_STRONG.propValue.equalsIgnoreCase(cacheTestTypeStr))
                    return CLUSTERED_STRONG;
                else
                    throw new IllegalArgumentException("CacheTestType [" + ((null != cacheTestTypeStr) ? cacheTestTypeStr : "null") + "] is not valid");
            } else {
                return DEFAULT_CACHETEST_TYPE;
            }
        }
    }

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
        private CustomPublicKey obj;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CustomPublicKey that = (CustomPublicKey) o;

            if (att1 != null ? !att1.equals(that.att1) : that.att1 != null) return false;
            if (att2 != null ? !att2.equals(that.att2) : that.att2 != null) return false;
            if (att3 != null ? !att3.equals(that.att3) : that.att3 != null) return false;
            if (att4 != null ? !att4.equals(that.att4) : that.att4 != null) return false;
            if (obj != null ? !obj.equals(that.obj) : that.obj != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = att1 != null ? att1.hashCode() : 0;
            result = 31 * result + (att2 != null ? att2.hashCode() : 0);
            result = 31 * result + (att3 != null ? att3.hashCode() : 0);
            result = 31 * result + (att4 != null ? att4.hashCode() : 0);
            result = 31 * result + (obj != null ? obj.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CustomPublicKey{" +
                    "att1='" + att1 + '\'' +
                    ", att2=" + att2 +
                    ", att3=" + att3 +
                    ", att4=" + att4 +
                    ", obj=" + obj +
                    '}';
        }

        public static CustomPublicKey generateRandom() {
            return generateRandom(true);
        }

        public static CustomPublicKey generateRandom(boolean createInnerObject){
            Random rnd = new Random(System.currentTimeMillis());
            CustomPublicKey customPublicKey = new CustomPublicKey();
            customPublicKey.att1 = generateRandomText(rnd,100);
            customPublicKey.att2 = rnd.nextLong();
            customPublicKey.att3 = rnd.nextInt();
            customPublicKey.att4 = rnd.nextFloat();
            if(createInnerObject)
                customPublicKey.obj = CustomPublicKey.generateRandom(false);

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
        String valStr = System.getProperty(ENV_CACHEKEY_TYPE, DEFAULT_CACHEKEY_TYPE.getPropValue());
        return CacheKeyType.valueOfIgnoreCase(valStr).getCacheKey();
    }

    public Ehcache getCache(){
        return cache;
    }

    public static void cacheStart() throws Exception {
        logger.debug("============ cacheStart ====================");

        String valStr = System.getProperty(ENV_CACHETEST_TYPE);
        CacheTestType cacheTestType = CacheTestType.valueOfIgnoreCase(valStr);

        cm = getCacheManager(cacheTestType.getCacheManagerName(), cacheTestType.getCacheConfigPath());
    }

    public static void cacheShutdown() throws Exception {
        logger.debug("============ cacheShutdown ====================");
        if(null != cm)
            cm.shutdown();

        cm = null;
    }

    public void cacheSetUp() throws Exception {
        logger.debug("============ cacheSetUp ====================");
        String valStr = System.getProperty(ENV_CACHETEST_TYPE);
        CacheTestType cacheTestType = CacheTestType.valueOfIgnoreCase(valStr);

        try {
            cache = cm.getCache(cacheTestType.getCacheName());
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (ClassCastException e) {
            e.printStackTrace();
        } catch (CacheException e) {
            e.printStackTrace();
        }

        if (cache == null) {
            throw new IllegalArgumentException("Could not find the cache " + cacheTestType.getCacheName());
        }

        //empty the cache
        cache.removeAll();
    }

    public void cacheCleanUp(){
        logger.debug("============ cacheCleanUp ====================");
        if(null != cache)
            cache.removeAll();
        cache = null;
    }

    public static long generateBigInputFile() throws IOException {
        long fileChecksum = 0L;

        try (
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(IN_FILE_PATH)), new CRC32());
        ) {
            logger.debug("============ Generate Initial Big File ====================");
            logger.debug("Path: " + IN_FILE_PATH);

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

            logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            logger.debug("CheckSum = " + fileChecksum);
            logger.debug("============================================");
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

    public static void sysPropDefaultSetup() throws Exception {
        if(null == System.getProperty(PropertyUtils.PROP_CONCURRENCY_MODE))
            System.setProperty(PropertyUtils.PROP_CONCURRENCY_MODE, PropertyUtils.DEFAULT_CONCURRENCY_MODE.getPropValue());
        if(null == System.getProperty(PropertyUtils.PROP_INPUTSTREAM_BUFFERSIZE))
            System.setProperty(PropertyUtils.PROP_INPUTSTREAM_BUFFERSIZE, new Integer(PropertyUtils.DEFAULT_INPUTSTREAM_BUFFER_SIZE).toString());
        if(null == System.getProperty(PropertyUtils.PROP_INPUTSTREAM_OPEN_TIMEOUTS))
            System.setProperty(PropertyUtils.PROP_INPUTSTREAM_OPEN_TIMEOUTS, new Long(PropertyUtils.DEFAULT_INPUTSTREAM_OPEN_TIMEOUT).toString());
        if(null == System.getProperty(PropertyUtils.PROP_INPUTSTREAM_ALLOW_NULLSTREAM))
            System.setProperty(PropertyUtils.PROP_INPUTSTREAM_ALLOW_NULLSTREAM, new Boolean(PropertyUtils.DEFAULT_INPUTSTREAM_ALLOW_NULLSTREAM).toString());

        if(null == System.getProperty(PropertyUtils.PROP_OUTPUTSTREAM_BUFFERSIZE))
            System.setProperty(PropertyUtils.PROP_OUTPUTSTREAM_BUFFERSIZE, new Integer(PropertyUtils.DEFAULT_OUTPUTSTREAM_BUFFER_SIZE).toString());
        if(null == System.getProperty(PropertyUtils.PROP_OUTPUTSTREAM_OVERRIDE))
            System.setProperty(PropertyUtils.PROP_OUTPUTSTREAM_OVERRIDE, new Boolean(PropertyUtils.DEFAULT_OUTPUTSTREAM_OVERRIDE).toString());
        if(null == System.getProperty(PropertyUtils.PROP_OUTPUTSTREAM_OPEN_TIMEOUTS))
            System.setProperty(PropertyUtils.PROP_OUTPUTSTREAM_OPEN_TIMEOUTS, new Long(PropertyUtils.DEFAULT_OUTPUTSTREAM_OPEN_TIMEOUT).toString());
    }

    public static void sysPropDefaultCleanup() throws Exception {
        System.clearProperty(PropertyUtils.PROP_CONCURRENCY_MODE);

        System.clearProperty(PropertyUtils.PROP_INPUTSTREAM_BUFFERSIZE);
        System.clearProperty(PropertyUtils.PROP_INPUTSTREAM_OPEN_TIMEOUTS);
        System.clearProperty(PropertyUtils.PROP_INPUTSTREAM_ALLOW_NULLSTREAM);

        System.clearProperty(PropertyUtils.PROP_OUTPUTSTREAM_BUFFERSIZE);
        System.clearProperty(PropertyUtils.PROP_OUTPUTSTREAM_OVERRIDE);
        System.clearProperty(PropertyUtils.PROP_OUTPUTSTREAM_OPEN_TIMEOUTS);
    }

    public long copyFileToCache(final Object publicCacheKey) throws IOException {
        return copyFileToCache(publicCacheKey, PropertyUtils.getOutputStreamDefaultOverride());
    }

    public long copyFileToCache(final Object publicCacheKey, final boolean cacheWriteOverride) throws IOException {
        return copyFileToCache(publicCacheKey, cacheWriteOverride, PropertyUtils.getOutputStreamBufferSize());
    }

    public long copyFileToCache(final Object publicCacheKey, final boolean cacheWriteOverride, final int cacheWriteBufferSize) throws IOException {
        int fileReadBufferSize = 32 * 1024;
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int copyBufferSize = 32*1024;

        logger.debug("============ copyFileToCache ====================");
        logger.debug("Before Cache Size = " + cache.getSize());

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),fileReadBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(EhcacheIOStreams.getOutputStream(cache, publicCacheKey, cacheWriteOverride, cacheWriteBufferSize),new CRC32())
        )
        {
            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + cache.getSize());
        logger.debug("============================================");

        return outputChecksum;
    }

    public long copyFileToCacheStream(OutputStream os) throws IOException {
        int fileReadBufferSize = 32 * 1024;
        long start = 0L, end = 0L;
        long inputChecksum = 0L;
        int copyBufferSize = 32*1024;

        logger.debug("============ copyFileToCache ====================");
        logger.debug("Before Cache Size = " + cache.getSize());

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),fileReadBufferSize),new CRC32());
        )
        {
            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d",inputChecksum));
        logger.debug("After Cache Size = " + cache.getSize());
        logger.debug("============================================");

        return inputChecksum;
    }

    public long readFileFromDisk() throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int copyBufferSize = 32*1024;

        logger.debug("============ readFileFromDisk ====================");

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH)), new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
        )
        {
            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();

            logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            logger.debug(String.format("CheckSums Input: %d // Output = %d", inputChecksum, outputChecksum));
            logger.debug("============================================");
        }

        return outputChecksum;
    }

    public long readFileFromCache(final Object publicCacheKey) throws IOException {
        return readFileFromCache(publicCacheKey, PropertyUtils.getInputStreamBufferSize());
    }

    public long readFileFromCache(final Object publicCacheKey, int cacheReadBufferSize) throws IOException {
        int copyBufferSize = 64 * 1024; //copy buffer size *smaller* than ehcache input stream internal buffer to make sure it works that way
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        logger.debug("============ readFileFromCache ====================");

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(cache, publicCacheKey, false, cacheReadBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
        )
        {
            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        Assert.assertEquals(inputChecksum, outputChecksum);

        return outputChecksum;
    }

    public long readFileFromCacheStream(final InputStream is) throws IOException {
        int copyBufferSize = 64 * 1024; //copy buffer size *smaller* than ehcache input stream internal buffer to make sure it works that way
        long start = 0L, end = 0L;
        long outputChecksum = 0L;

        logger.debug("============ readFileFromCacheStream ====================");

        try (
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
        )
        {
            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Output = %d",outputChecksum));
        return outputChecksum;
    }

    private static CacheManager getCacheManager(String cacheManagerName, String resourcePath) {
        CacheManager cm = null;
        logger.debug("============ getCacheManager ====================");

        if (null == (cm = CacheManager.getCacheManager(cacheManagerName))) {
            String configLocationToLoad = null;
            if (null != resourcePath && !"".equals(resourcePath)) {
                configLocationToLoad = resourcePath;
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

    public void runInThreads(List<Callable<Long>> callables, List<AtomicReference<Long>> callableResults, List<AtomicReference<Throwable>> exceptions) throws InterruptedException {
        long start = 0L, end = 0L;
        logger.info("============ runInThreads ====================");

        if(null == callables)
            throw new IllegalStateException("must provides some operations to run...");

        if(null != callableResults && callables.size() != callableResults.size())
            throw new IllegalStateException("must provides the same number of callableResults as the number of callables");

        if(null != exceptions && callables.size() != exceptions.size())
            throw new IllegalStateException("must provides the same number of exception counters as the number of callables");

        final List<ThreadWorker> workerList = new ArrayList<>(callables.size());
        final CountDownLatch stopLatch = new CountDownLatch(callables.size());

        //add first worker
        int count = 0;
        for(int i = 0; i < callables.size(); i++) {
            workerList.add(new ThreadWorker(callables.get(i), stopLatch, callableResults.get(i), exceptions.get(i)));
        }

        //start the workers
        start = System.nanoTime();
        for (ThreadWorker worker : workerList) {
            worker.start();
        }

        //wait that all operations are finished
        stopLatch.await();
        end = System.nanoTime();
        logger.info("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.info("============ end runInThreads ====================");
    }

    public class ThreadWorker<R> extends Thread {
        private final Callable<R> callable;
        private final CountDownLatch doneLatch;
        private AtomicReference<R> callableResult;
        private AtomicReference<Throwable> exception;

        public ThreadWorker(Callable<R> callable, CountDownLatch doneLatch, AtomicReference<R> callableResult, AtomicReference<Throwable> exception) {
            super();
            this.callable = callable;
            this.doneLatch = doneLatch;
            this.callableResult = callableResult;
            this.exception = exception;
        }

        @Override
        public void run() {
            try {
                callableResult.set(callable.call());
            } catch (Exception e) {
                if(null != exception)
                    exception.set(e);
                logger.debug(e.getMessage(),e);
            } finally{
                doneLatch.countDown();
            }
        }
    }
}
