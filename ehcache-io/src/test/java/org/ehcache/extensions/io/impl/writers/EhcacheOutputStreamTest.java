package org.ehcache.extensions.io.impl.writers;

import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

@RunWith(Parameterized.class)
public class EhcacheOutputStreamTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheOutputStreamTest.class);

    private static StreamCopyResultDescriptor bigInputFileDescriptor = null;
    private StreamCopyResultDescriptor fileFromDisk = null;

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        logger.debug("============ Starting EhcacheOutputStreamTest ====================");
        sysPropDefaultSetup();
        cacheStart();
        bigInputFileDescriptor = generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        sysPropDefaultCleanup();
        bigInputFileDescriptor = null;
        logger.debug("============ Finished EhcacheOutputStreamTest ====================");
    }

    @Before
    public void setup() throws Exception {
        setupParameterizedProperties();
        cacheSetUp();
        fileFromDisk = readFileFromDisk();
        printAllTestProperties();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        cleanBigOutputFile();
        fileFromDisk = null;
        cleanupParameterizedProperties();
    }

    public long testCopyFileToCacheByteByByte(Boolean override, Integer bufferSize) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;

        logger.info("============ testCopyFileToCacheByteByByte With Override=" + override + " ====================");

        OutputStream ehcacheOutputStream;
        if(null == override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());
        else if (null != override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue());
        else if (null == override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), PropertyUtils.getOutputStreamDefaultOverride(), bufferSize.intValue());
        else if (null != override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue(), bufferSize.intValue());
        else
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(ehcacheOutputStream,new CRC32())
        )
        {
            start = System.nanoTime();
            pipeStreamsByteByByte(is, os);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);

        return outputChecksum;
    }

    @Test
    public void testCopyFileToCacheByteByByteDefaults() throws IOException {
        logger.info("============ testCopyFileToCacheByteByByteDefaults ====================");

        long outputChecksum = testCopyFileToCacheByteByByte(null, null); //this should be same as override!!

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheByteByByteOverride() throws IOException {
        logger.info("============ testCopyFileToCacheByteByByteOverride ====================");

        long outputChecksum = testCopyFileToCacheByteByByte(true, null);

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheByteByByteAppend() throws IOException {
        logger.info("============ testCopyFileToCacheByteByByteAppend ====================");

        long outputChecksum = testCopyFileToCacheByteByByte(false, null);

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheByteByByteOverrideMultipleTimes() throws IOException {
        logger.info("============ testCopyFileToCacheByteByByteOverrideMultipleTimes ====================");

        int copyIterations = 5;

        testCopyFileToCacheByteByByteOverride();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheByteByByte(true, null);

            //get the file from cache again
            StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        }
    }

    @Test
    public void testCopyFileToCacheByteByByteAppendMultipleTimes() throws IOException {
        logger.info("============ testCopyFileToCacheByteByByteAppendMultipleTimes ====================");

        int copyIterations = 5;

        testCopyFileToCacheByteByByteAppend();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheByteByByte(false, null);

            //get the file from cache again
            StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        }
    }

    public long testCopyFileToCacheWithBuffer(Boolean override, Integer bufferSize) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;
        int copyBufferSize = 128*1024;

        OutputStream ehcacheOutputStream;
        if(null == override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());
        else if (null != override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue());
        else if (null == override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), PropertyUtils.getOutputStreamDefaultOverride(), bufferSize.intValue());
        else if (null != override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue(), bufferSize.intValue());
        else
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(ehcacheOutputStream,new CRC32())
        )
        {
            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);

        return outputChecksum;
    }

    @Test
    public void testCopyFileToCacheWithBufferDefaults() throws IOException {
        logger.info("============ testCopyFileToCacheWithBufferDefaults ====================");

        long outputChecksum = testCopyFileToCacheWithBuffer(null, null); //this should be same as override!!

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheWithBufferOverride() throws IOException {
        logger.info("============ testCopyFileToCacheWithBufferOverride ====================");

        long outputChecksum = testCopyFileToCacheWithBuffer(true, null);

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheWithBufferAppend() throws IOException {
        logger.info("============ testCopyFileToCacheWithBufferAppend ====================");

        long outputChecksum = testCopyFileToCacheWithBuffer(false, null);

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheWithBufferOverrideMultipleTimes() throws IOException {
        logger.info("============ testCopyFileToCacheWithBufferOverrideMultipleTimes ====================");

        int copyIterations = 5;

        testCopyFileToCacheWithBufferOverride();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheWithBuffer(true, null);

            //get the file from cache again
            StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        }
    }

    @Test
    public void testCopyFileToCacheWithBufferAppendMultipleTimes() throws IOException {
        logger.info("============ testCopyFileToCacheWithBufferAppendMultipleTimes ====================");

        int copyIterations = 5;

        testCopyFileToCacheWithBufferAppend();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheWithBuffer(false, null);

            //get the file from cache again
            StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());

        }
    }

    public long testCopyFileToCacheInOneShot(Boolean override, Integer bufferSize) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;

        OutputStream ehcacheOutputStream;
        if(null == override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());
        else if (null != override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue());
        else if (null == override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), PropertyUtils.getOutputStreamDefaultOverride(), bufferSize.intValue());
        else if (null != override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue(), bufferSize.intValue());
        else
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(ehcacheOutputStream,new CRC32());
                ByteArrayOutputStream bos = new ByteArrayOutputStream(new Double(bigInputFileDescriptor.getToSizeBytes() * 1.2).intValue())
        )
        {
            //first stream from file to ByteArrayOutputStream
            pipeStreamsByteByByte(is, bos);
            byte[] fullBytes = bos.toByteArray();

            start = System.nanoTime();
            os.write(fullBytes);
            end = System.nanoTime();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);

        return outputChecksum;
    }

    @Test
    public void testCopyFileToCacheInOneShotDefaults() throws IOException {
        logger.info("============ testCopyFileToCacheInOneShotDefaults ====================");

        long outputChecksum = testCopyFileToCacheInOneShot(null, null); //this should be same as override!!

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheInOneShotOverride() throws IOException {
        logger.info("============ testCopyFileToCacheInOneShotOverride ====================");

        long outputChecksum = testCopyFileToCacheInOneShot(true, null);

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheInOneShotAppend() throws IOException {
        logger.info("============ testCopyFileToCacheInOneShotAppend ====================");

        long outputChecksum = testCopyFileToCacheInOneShot(false, null);

        //get the file from cache again
        StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        Assert.assertEquals(fileFromDisk.getFromChecksum(), outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheInOneShotOverrideMultipleTimes() throws IOException {
        logger.info("============ testCopyFileToCacheInOneShotOverrideMultipleTimes ====================");

        int copyIterations = 5;

        testCopyFileToCacheInOneShotOverride();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheInOneShot(true, null);

            //get the file from cache again
            StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        }
    }

    @Test
    public void testCopyFileToCacheInOneShotAppendMultipleTimes() throws IOException {
        logger.info("============ testCopyFileToCacheInOneShotAppendMultipleTimes ====================");

        int copyIterations = 5;

        testCopyFileToCacheInOneShotAppend();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheInOneShot(false, null);

            //get the file from cache again
            StreamCopyResultDescriptor fileFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(fileFromDisk.getFromChecksum(), fileFromCache.getFromChecksum());
        }
    }
}