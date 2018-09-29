package org.ehcache.extensions.io.impl;

import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

@RunWith(Parameterized.class)
public class EhcacheInputStreamTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheInputStreamTest.class);

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        logger.debug("============ Starting EhcacheInputStreamTest ====================");
        sysPropDefaultSetup();
        cacheStart();
        generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        sysPropDefaultCleanup();
        logger.debug("============ Finished EhcacheInputStreamTest ====================");
    }

    @Before
    public void setup() throws Exception {
        setupParameterizedProperties();
        cacheSetUp();
        printAllTestProperties();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        cleanBigOutputFile();
        cleanupParameterizedProperties();
    }

    @Test
    public void copyCacheToFileLargeReadBufferSmallCacheChunks() throws Exception {
        logger.info("============ copyCacheToFileLargeReadBufferSmallCacheChunks ====================");

        int inBufferSize = 512 * 1024; //ehcache input stream large internal buffer
        int outBufferSize = 12 * 1024; //small chunks in cache
        int copyBufferSize = 64 * 1024; //copy buffer size *smaller* than ehcache input stream internal buffer to make sure it works that way
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        long inputFileCheckSum = copyFileToCache(getCacheKey(), true, outBufferSize);

        Assert.assertTrue(getCache().getSize() > 0);

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey(), false, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            logger.debug("Before Cache Size = " + getCache().getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileSmallReadBufferLargeCacheChunks() throws Exception {
        logger.info("============ copyCacheToFileSmallReadBufferLargeCacheChunks ====================");

        int inBufferSize = 17 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 769 * 1024; //large cache chunks
        int copyBufferSize = 357 * 1024; //copy buffer size *larger* than ehcache input stream internal buffer to make sure it works that way

        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        long inputFileCheckSum = copyFileToCache(getCacheKey(), true, outBufferSize);

        Assert.assertTrue(getCache().getSize() > 0);

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey(), false, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            logger.debug("Before Cache Size = " + getCache().getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffers() throws Exception {
        logger.info("============ copyCacheToFileUsingStreamDefaultBuffers ====================");

        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        long inputFileCheckSum = copyFileToCache(getCacheKey(), true);

        Assert.assertTrue(getCache().getSize() > 0);

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey()),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            logger.debug("Before Cache Size = " + getCache().getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffersByteByByte() throws Exception {
        logger.info("============ copyCacheToFileUsingStreamDefaultBuffersByteByByte ====================");

        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        long inputFileCheckSum = copyFileToCache(getCacheKey(), true);

        Assert.assertTrue(getCache().getSize() > 0);

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey()),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            logger.debug("Before Cache Size = " + getCache().getSize());

            start = System.nanoTime();;
            pipeStreamsByteByByte(is, os);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileNoCacheKeyAllowsNullStream() throws Exception {
        logger.info("============ copyCacheToFileNoCacheKeyAllowsNullStream ====================");

        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        long inputFileCheckSum = copyFileToCache(getCacheKey(), true);

        Assert.assertTrue(getCache().getSize() > 0);

        final String cacheKey = "something-else";

        InputStream is = EhcacheIOStreams.getInputStream(getCache(), cacheKey, true);
        Assert.assertEquals(is, null);
    }

    @Test
    public void copyCacheToFileNoCacheKeyNoNullStream() throws Exception {
        logger.info("============ copyCacheToFileNoCacheKeyNoNullStream ====================");

        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        long inputFileCheckSum = copyFileToCache(getCacheKey(), true);

        Assert.assertTrue(getCache().getSize() > 0);

        final String cacheKeyNotExist = getCacheKey() + "doesNotExist";

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), cacheKeyNotExist, false),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            int beforeCacheSize = getCache().getSize();
            logger.debug("Before Cache Size = " + beforeCacheSize);

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertNotEquals(inputFileCheckSum, outputChecksum);
        Assert.assertNotEquals(inputFileCheckSum, inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertEquals(0, outputChecksum);
        Assert.assertEquals(0, inputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
        Assert.assertEquals(0, Files.size(OUT_FILE_PATH));
    }
}