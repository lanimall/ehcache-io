package org.ehcache.extensions.io;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

public class EhcacheInputStreamTest extends EhcacheStreamingTestsBase {

    private long inputFileCheckSum = -1L;

    @Before
    public void copyFileToCache() throws Exception {
        int inBufferSize = 32 * 1024;
        int copyBufferSize = 128 * 1024;
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        System.out.println("============ copyFileToCache ====================");
        System.out.println("Before Cache Size = " + cache.getSize());

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key, false),new CRC32())
        )
        {
            start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();

            this.inputFileCheckSum = is.getChecksum().getValue();
            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + cache.getSize());
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);
    }

    @After
    public void emptyCache() throws Exception {
        cache.removeAll();

        if(Files.exists(OUT_FILE_PATH))
            Files.delete(OUT_FILE_PATH);
    }

    @Test
    public void copyCacheToFileUsingStreamSmallerCopyBuffer() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 128 * 1024;
        int copyBufferSize = 64 * 1024; //copy buffer size *smaller* than ehcache input stream internal buffer to make sure it works that way
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamSmallerCopyBuffer ====================");
            System.out.println("Before Cache Size = " + cache.getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + cache.getSize());
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputChecksum, inputFileCheckSum);
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
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key, inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamLargerCopyBuffer ====================");
            System.out.println("Before Cache Size = " + cache.getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + cache.getSize());
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputChecksum, inputFileCheckSum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffers() throws Exception {
        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffers ====================");
            System.out.println("Before Cache Size = " + cache.getSize());

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + cache.getSize());
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputChecksum, inputFileCheckSum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffersByteByByte() throws Exception {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffersByteByByte ====================");
            System.out.println("Before Cache Size = " + cache.getSize());

            start = System.nanoTime();;
            pipeStreamsByteByByte(is, os);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + cache.getSize());
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertEquals(inputFileCheckSum, outputChecksum);
        Assert.assertEquals(inputChecksum, inputFileCheckSum);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }

    @Test
    public void copyCacheToFileNoCacheKey() throws Exception {
        int copyBufferSize = 512 * 1024; //copy buffer size
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        final String cacheKey = "something-else";
        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cacheKey),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileNoCacheKey ====================");

            int beforeCacheSize = cache.getSize();
            System.out.println("Before Cache Size = " + beforeCacheSize);

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();
        }

        System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
        System.out.println(String.format("CheckSums Input: %d // Output = %d",inputChecksum,outputChecksum));
        System.out.println("After Cache Size = " + cache.getSize());
        System.out.println("============================================");

        Assert.assertEquals(inputChecksum, outputChecksum);
        Assert.assertEquals(0, outputChecksum);
        Assert.assertEquals(inputChecksum, 0);
        Assert.assertTrue(Files.exists(OUT_FILE_PATH));
    }
}