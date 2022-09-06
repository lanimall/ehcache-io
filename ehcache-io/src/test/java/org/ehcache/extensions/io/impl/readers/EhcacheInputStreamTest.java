package org.ehcache.extensions.io.impl.readers;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.CountingOutputStream;
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
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

@RunWith(Parameterized.class)
public class EhcacheInputStreamTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheInputStreamTest.class);

    private static StreamCopyResultDescriptor bigInputFileDescriptor = null;

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        logger.debug("============ Starting EhcacheInputStreamTest ====================");
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

        int cacheSize = 0;
        Assert.assertEquals(cacheSize, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey(), true, outBufferSize, copyBufferSize);
        cacheSize = getCache().getSize();
        Assert.assertTrue(cacheSize > 0);

        int beforeReadAvailable;
        int afterFullReadAvailable;
        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey(), false, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            logger.debug("Before Cache Size = " + getCache().getSize());

            //save the available value before the reads
            beforeReadAvailable = is.available();

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            //save the available value after the full read
            afterFullReadAvailable = is.available();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(bigInputFileDescriptor.getToSizeBytes(), beforeReadAvailable);
        Assert.assertEquals(0, afterFullReadAvailable);

        Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), outputChecksum);
        Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), inputChecksum);
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

        int cacheSize = 0;
        Assert.assertEquals(cacheSize, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey(), true, outBufferSize, copyBufferSize);
        cacheSize = getCache().getSize();
        Assert.assertTrue(cacheSize > 0);

        int beforeReadAvailable;
        int afterFullReadAvailable;
        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey(), false, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            logger.debug("Before Cache Size = " + getCache().getSize());

            //save the available value before the reads
            beforeReadAvailable = is.available();

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            //save the available value after the full read
            afterFullReadAvailable = is.available();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(bigInputFileDescriptor.getToSizeBytes(), beforeReadAvailable);
        Assert.assertEquals(0, afterFullReadAvailable);

        Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), outputChecksum);
        Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffers() throws Exception {
        logger.info("============ copyCacheToFileUsingStreamDefaultBuffers ====================");

        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        long readBytes = 0L, writtenBytes = 0L;

        int cacheSize = 0;
        Assert.assertEquals(cacheSize, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey(), true);
        cacheSize = getCache().getSize();
        Assert.assertTrue(cacheSize > 0);

        int beforeReadAvailable;
        int afterFullReadAvailable;
        try (
                InputStream rawIs = EhcacheIOStreams.getInputStream(getCache(), getCacheKey());
                OutputStream rawOs = new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH));

                CheckedInputStream ckIs = new CheckedInputStream(rawIs,new CRC32());
                CheckedOutputStream ckOs = new CheckedOutputStream(rawOs, new CRC32());

                CountingInputStream countingIs = new CountingInputStream(ckIs);
                CountingOutputStream countingOs = new CountingOutputStream(ckOs);

                InputStream is = countingIs;
                OutputStream os = countingOs;
        )
        {
            logger.debug("Before Cache Size = " + getCache().getSize());

            //save the available value before the reads
            beforeReadAvailable = is.available();

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            //save the available value after the full read
            afterFullReadAvailable = is.available();

            readBytes = countingIs.getByteCount();
            writtenBytes = countingOs.getByteCount();

            inputChecksum = ckIs.getChecksum().getValue();
            outputChecksum = ckOs.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(readBytes, writtenBytes);
        Assert.assertEquals(bigInputFileDescriptor.getToSizeBytes(), readBytes);
        Assert.assertEquals(bigInputFileDescriptor.getToSizeBytes(), writtenBytes);

        Assert.assertEquals(bigInputFileDescriptor.getToSizeBytes(), beforeReadAvailable);
        Assert.assertEquals(0, afterFullReadAvailable);

        Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), outputChecksum);
        Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffersByteByByte() throws Exception {
        logger.info("============ copyCacheToFileUsingStreamDefaultBuffersByteByByte ====================");

        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        int cacheSize = 0;
        Assert.assertEquals(cacheSize, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey(), true);
        cacheSize = getCache().getSize();
        Assert.assertTrue(cacheSize > 0);

        int beforeReadAvailable;
        int afterFullReadAvailable;
        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), getCacheKey()),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            logger.debug("Before Cache Size = " + getCache().getSize());

            //save the available value before the reads
            beforeReadAvailable = is.available();

            start = System.nanoTime();;
            pipeStreamsByteByByte(is, os);
            end = System.nanoTime();;

            //save the available value after the full read
            afterFullReadAvailable = is.available();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(bigInputFileDescriptor.getToSizeBytes(), beforeReadAvailable);
        Assert.assertEquals(0, afterFullReadAvailable);

        Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), outputChecksum);
        Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileNoCacheKeyAllowsNullStream() throws Exception {
        logger.info("============ copyCacheToFileNoCacheKeyAllowsNullStream ====================");

        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        int cacheSize = 0;
        Assert.assertEquals(cacheSize, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey(), true);
        cacheSize = getCache().getSize();
        Assert.assertTrue(cacheSize > 0);

        final String cacheKey = "something-else";

        InputStream is = EhcacheIOStreams.getInputStream(getCache(), cacheKey, true);
        Assert.assertEquals(is, null);
        Assert.assertEquals(cacheSize, getCache().getSize());
    }

    @Test
    public void copyCacheToFileNoCacheKeyNoNullStream() throws Exception {
        logger.info("============ copyCacheToFileNoCacheKeyNoNullStream ====================");

        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        int cacheSize = 0;
        Assert.assertEquals(cacheSize, getCache().getSize()); // should be 0 now

        //first, copy file to cache
        StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey(), true);
        cacheSize = getCache().getSize();
        Assert.assertTrue(cacheSize > 0);

        final String cacheKeyNotExist = getCacheKey() + "doesNotExist";

        int beforeReadAvailable;
        int afterFullReadAvailable;
        try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), cacheKeyNotExist, false),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            int beforeCacheSize = getCache().getSize();
            logger.debug("Before Cache Size = " + beforeCacheSize);

            //save the available value before the reads
            beforeReadAvailable = is.available();

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            //save the available value after the full read
            afterFullReadAvailable = is.available();

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        logger.debug(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        logger.debug("After Cache Size = " + getCache().getSize());
        logger.debug("============================================");

        Assert.assertEquals(0, beforeReadAvailable);
        Assert.assertEquals(0, afterFullReadAvailable);

        Assert.assertNotEquals(copyFileToCacheDesc.getToChecksum(), outputChecksum);
        Assert.assertNotEquals(copyFileToCacheDesc.getToChecksum(), inputChecksum);
        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertEquals(0, outputChecksum);
        Assert.assertEquals(0, inputChecksum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
        Assert.assertEquals(0, Files.size(OUT_FILE_PATH));
        Assert.assertEquals(cacheSize, getCache().getSize());
    }
}