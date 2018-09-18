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

    private long inputFileCheckSum = -1L;

    @Before
    public void setup() throws Exception {
        setupParameterizedProperties();
        cacheSetUp();
        inputFileCheckSum = copyFileToCache(getCacheKey());
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        cleanBigOutputFile();
        inputFileCheckSum = -1L;
        cleanupParameterizedProperties();
    }

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

    @Test
    public void copyCacheToFileUsingStreamSmallerCopyBuffer() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 128 * 1024;
        int copyBufferSize = 64 * 1024; //copy buffer size *smaller* than ehcache input stream internal buffer to make sure it works that way
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey(), false, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            logger.info("============ copyCacheToFileUsingStreamSmallerCopyBuffer ====================");
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
    public void copyCacheToFileUsingStreamLargerCopyBuffer() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 128 * 1024;
        int copyBufferSize = 357 * 1024; //copy buffer size *larger* than ehcache input stream internal buffer to make sure it works that way
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey(), false, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            logger.info("============ copyCacheToFileUsingStreamLargerCopyBuffer ====================");
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
        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey()),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            logger.info("============ copyCacheToFileUsingStreamDefaultBuffers ====================");
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
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey()),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            logger.info("============ copyCacheToFileUsingStreamDefaultBuffersByteByByte ====================");
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
        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        final String cacheKey = "something-else";

        InputStream is = EhcacheIOStreams.getInputStream(getCache(), cacheKey, true);
        Assert.assertEquals(is, null);
    }

    @Test
    public void copyCacheToFileNoCacheKeyNoNullStream() throws Exception {
        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        final String cacheKeyNotExist = getCacheKey() + "doesNotExist";

        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), cacheKeyNotExist, false),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            logger.info("============ copyCacheToFileNoCacheKey ====================");

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