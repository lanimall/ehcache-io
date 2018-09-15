package org.ehcache.extensions.io.impl;

import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.junit.*;
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

public class EhcacheOutputStreamTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheOutputStreamTest.class);

    private long inputFileCheckSum = -1L;

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        System.out.println("============ Starting EhcacheOutputStreamTest ====================");
        sysPropDefaultSetup();
        cacheStart();
        generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        sysPropDefaultCleanup();
        System.out.println("============ Finished EhcacheOutputStreamTest ====================");
    }

    @Before
    public void setup() throws Exception {
        cacheSetUp();
        inputFileCheckSum = readFileFromDisk();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        cleanBigOutputFile();
        inputFileCheckSum = -1L;
    }

    public long testCopyFileToCacheByteByByte(Boolean override, Integer bufferSize) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;

        System.out.println("============ testCopyFileToCacheByteByByte With Override=" + override + " ====================");

        OutputStream ehcacheOutputStream;
        if(null == override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());
        else if (null != override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue());
        else if (null == override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), PropertyUtils.outputStreamDefaultOverride, bufferSize.intValue());
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

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);

        return outputChecksum;
    }

    @Test
    public void testCopyFileToCacheByteByByteDefaults() throws IOException {
        long outputChecksum = testCopyFileToCacheByteByByte(null, null); //this should be same as override!!

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheByteByByteOverride() throws IOException {
        long outputChecksum = testCopyFileToCacheByteByByte(true, null);

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheByteByByteAppend() throws IOException {
        long outputChecksum = testCopyFileToCacheByteByByte(false, null);

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheByteByByteOverrideMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheByteByByteOverride();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheByteByByte(true, null);

            //get the file from cache again
            long checksumFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        }
    }

    @Test
    public void testCopyFileToCacheByteByByteAppendMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheByteByByteAppend();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheByteByByte(false, null);

            //get the file from cache again
            long checksumFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(inputFileCheckSum, checksumFromCache);
        }
    }

    public long testCopyFileToCacheWithBuffer(Boolean override, Integer bufferSize) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;
        int copyBufferSize = 128*1024;

        System.out.println("============ testCopyFileToCacheWithBuffer With Override=" + override + " ====================");

        OutputStream ehcacheOutputStream;
        if(null == override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());
        else if (null != override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue());
        else if (null == override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), PropertyUtils.outputStreamDefaultOverride, bufferSize.intValue());
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

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);

        return outputChecksum;
    }

    @Test
    public void testCopyFileToCacheWithBufferDefaults() throws IOException {
        long outputChecksum = testCopyFileToCacheWithBuffer(null, null); //this should be same as override!!

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheWithBufferOverride() throws IOException {
        long outputChecksum = testCopyFileToCacheWithBuffer(true, null);

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheWithBufferAppend() throws IOException {
        long outputChecksum = testCopyFileToCacheWithBuffer(false, null);

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheWithBufferOverrideMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheWithBufferOverride();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheWithBuffer(true, null);

            //get the file from cache again
            long checksumFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        }
    }

    @Test
    public void testCopyFileToCacheWithBufferAppendMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheWithBufferAppend();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheWithBuffer(false, null);

            //get the file from cache again
            long checksumFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(inputFileCheckSum, checksumFromCache);

        }
    }

    public long testCopyFileToCacheInOneShot(Boolean override, Integer bufferSize) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;

        System.out.println("============ testCopyFileToCacheInOneShot ====================");

        OutputStream ehcacheOutputStream;
        if(null == override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());
        else if (null != override && null == bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue());
        else if (null == override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), PropertyUtils.outputStreamDefaultOverride, bufferSize.intValue());
        else if (null != override && null != bufferSize)
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey(), override.booleanValue(), bufferSize.intValue());
        else
            ehcacheOutputStream = EhcacheIOStreams.getOutputStream(getCache(), getCacheKey());

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(ehcacheOutputStream,new CRC32());
                ByteArrayOutputStream bos = new ByteArrayOutputStream(new Double(IN_FILE_SIZE * 1.2).intValue())
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

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);

        return outputChecksum;
    }

    @Test
    public void testCopyFileToCacheInOneShotDefaults() throws IOException {
        long outputChecksum = testCopyFileToCacheInOneShot(null, null); //this should be same as override!!

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheInOneShotOverride() throws IOException {
        long outputChecksum = testCopyFileToCacheInOneShot(true, null);

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheInOneShotAppend() throws IOException {
        long outputChecksum = testCopyFileToCacheInOneShot(false, null);

        //get the file from cache again
        long checksumFromCache = readFileFromCache(getCacheKey());

        Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheInOneShotOverrideMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheInOneShotOverride();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheInOneShot(true, null);

            //get the file from cache again
            long checksumFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(inputFileCheckSum, checksumFromCache);
        }
    }

    @Test
    public void testCopyFileToCacheInOneShotAppendMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheInOneShotAppend();
        int initialCacheSizeAfter1 = getCache().getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheInOneShot(false, null);

            //get the file from cache again
            long checksumFromCache = readFileFromCache(getCacheKey());

            int newCacheSize = getCache().getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(inputFileCheckSum, checksumFromCache);
        }
    }
}