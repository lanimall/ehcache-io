package org.ehcache.extensions.io.impl;

import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

public class EhcacheInputStreamTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheInputStreamTest.class);

    private long inputFileCheckSum = -1L;

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        System.out.println("============ Starting EhcacheInputStreamTest ====================");
        sysPropDefaultSetup();
        cacheStart();
        generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        sysPropDefaultCleanup();
        System.out.println("============ Finished EhcacheInputStreamTest ====================");
    }

    @Before
    public void setup() throws Exception {
        cacheSetUp();
        inputFileCheckSum = copyFileToCache(getCacheKey());
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        cleanBigOutputFile();
        inputFileCheckSum = -1L;
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
            System.out.println("============ copyCacheToFileUsingStreamSmallerCopyBuffer ====================");
            System.out.println("Before Cache Size = " + getCache().getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + getCache().getSize());
        System.out.println("============================================");

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
            System.out.println("============ copyCacheToFileUsingStreamLargerCopyBuffer ====================");
            System.out.println("Before Cache Size = " + getCache().getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + getCache().getSize());
        System.out.println("============================================");

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
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffers ====================");
            System.out.println("Before Cache Size = " + getCache().getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + getCache().getSize());
        System.out.println("============================================");

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
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffersByteByByte ====================");
            System.out.println("Before Cache Size = " + getCache().getSize());

            start = System.nanoTime();;
            pipeStreamsByteByByte(is, os);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + getCache().getSize());
        System.out.println("============================================");

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
            System.out.println("============ copyCacheToFileNoCacheKey ====================");

            int beforeCacheSize = getCache().getSize();
            System.out.println("Before Cache Size = " + beforeCacheSize);

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + getCache().getSize());
        System.out.println("============================================");

        Assert.assertNotEquals(inputFileCheckSum, outputChecksum);
        Assert.assertNotEquals(inputFileCheckSum, inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertEquals(0, outputChecksum);
        Assert.assertEquals(0, inputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
        Assert.assertEquals(0, Files.size(OUT_FILE_PATH));
    }
}