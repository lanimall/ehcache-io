package org.ehcache.extensions.io;

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

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key),new CRC32())
        )
        {
            System.out.println("============ copyFileToCache ====================");

            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            this.inputFileCheckSum = is.getChecksum().getValue();
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingStreamSmallerCopyBuffer() throws Exception {
        boolean lockImmediately = false;
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 128 * 1024;
        int copyBufferSize = 64 * 1024; //copy buffer size *smaller* than ehcache input stream internal buffer to make sure it works that way

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key, inBufferSize, lockImmediately),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamSmallerCopyBuffer ====================");
            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(inputFileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), inputFileCheckSum);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingStreamLargerCopyBuffer() throws Exception {
        boolean lockImmediately = false;
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        int outBufferSize = 128 * 1024;
        int copyBufferSize = 357 * 1024; //copy buffer size *larger* than ehcache input stream internal buffer to make sure it works that way

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key, inBufferSize,lockImmediately),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH),outBufferSize), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamLargerCopyBuffer ====================");
            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(inputFileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), inputFileCheckSum);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffers() throws Exception {
        int copyBufferSize = 512 * 1024; //copy buffer size

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffers ====================");
            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(inputFileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), inputFileCheckSum);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileUsingStreamDefaultBuffersByteByByte() throws Exception {
        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffersByteByByte ====================");
            long start = System.nanoTime();;
            pipeStreamsByteByByte(is, os);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(inputFileCheckSum, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), inputFileCheckSum);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void copyCacheToFileNoCacheKey() throws Exception {
        int copyBufferSize = 512 * 1024; //copy buffer size
        final String cacheKey = "something-else";
        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cacheKey),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileNoCacheKey ====================");
            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(0, os.getChecksum().getValue());
            Assert.assertEquals(is.getChecksum().getValue(), 0);
            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }
}