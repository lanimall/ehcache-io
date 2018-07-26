package org.ehcache.extensions.io;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

public class EhcacheOutputStreamTest extends EhcacheStreamingTestsBase {

    private long inputFileCheckSum = -1L;

    @Before
    public void readFileFromDisk() throws Exception {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH)), new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
        )
        {
            System.out.println("============ readFileFromDisk ====================");

            int copyBufferSize = 32*1024;

            start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            end = System.nanoTime();;

            this.inputFileCheckSum = is.getChecksum().getValue();
            inputChecksum = is.getChecksum().getValue();
            outputChecksum = os.getChecksum().getValue();

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println(String.format("CheckSums Input: %d // Output = %d", inputChecksum, outputChecksum));
            System.out.println("============================================");

            Assert.assertEquals(inputChecksum, outputChecksum);
        }
    }

    public long testCopyFileToCacheByteByByte(boolean override) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;

        System.out.println("============ testCopyFileToCacheByteByByte With Override=" + override + " ====================");

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key,override),new CRC32())
        )
        {
            start = System.nanoTime();;
            pipeStreamsByteByByte(is, os);
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
    public void testCopyFileToCacheByteByByteOverride() throws IOException {
        long outputChecksum = testCopyFileToCacheByteByByte(true);

        //get the file from cache again
        long checksumFromCache = getFileChecksumFromCache();

        Assert.assertEquals(checksumFromCache, outputChecksum);
        Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        Assert.assertTrue(cache.getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheByteByByteAppend() throws IOException {
        long outputChecksum = testCopyFileToCacheByteByByte(false);

        //get the file from cache again
        long checksumFromCache = getFileChecksumFromCache();

        Assert.assertEquals(checksumFromCache, outputChecksum);
        Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        Assert.assertTrue(cache.getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheByteByByteOverrideMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheByteByByteOverride();
        int initialCacheSizeAfter1 = cache.getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheByteByByte(true);

            //get the file from cache again
            long checksumFromCache = getFileChecksumFromCache();

            int newCacheSize = cache.getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        }
    }

    @Test
    public void testCopyFileToCacheByteByByteAppendMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheByteByByteAppend();
        int initialCacheSizeAfter1 = cache.getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheByteByByte(false);

            //get the file from cache again
            long checksumFromCache = getFileChecksumFromCache();

            int newCacheSize = cache.getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(checksumFromCache, inputFileCheckSum);
        }
    }

    public long testCopyFileToCacheWithBuffer(boolean override) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;
        int copyBufferSize = 128*1024;

        System.out.println("============ testCopyFileToCacheWithBuffer With Override=" + override + " ====================");

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key, override),new CRC32())
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
    public void testCopyFileToCacheWithBufferOverride() throws IOException {
        long outputChecksum = testCopyFileToCacheWithBuffer(true);

        //get the file from cache again
        long checksumFromCache = getFileChecksumFromCache();

        Assert.assertEquals(checksumFromCache, outputChecksum);
        Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        Assert.assertTrue(cache.getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheWithBufferAppend() throws IOException {
        long outputChecksum = testCopyFileToCacheWithBuffer(false);

        //get the file from cache again
        long checksumFromCache = getFileChecksumFromCache();

        Assert.assertEquals(checksumFromCache, outputChecksum);
        Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        Assert.assertTrue(cache.getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheWithBufferOverrideMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheWithBufferOverride();
        int initialCacheSizeAfter1 = cache.getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheWithBuffer(true);

            //get the file from cache again
            long checksumFromCache = getFileChecksumFromCache();

            int newCacheSize = cache.getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        }
    }

    @Test
    public void testCopyFileToCacheWithBufferAppendMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheWithBufferAppend();
        int initialCacheSizeAfter1 = cache.getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheWithBuffer(false);

            //get the file from cache again
            long checksumFromCache = getFileChecksumFromCache();

            int newCacheSize = cache.getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(checksumFromCache, inputFileCheckSum);

        }
    }

    public long testCopyFileToCacheInOneShot(boolean override) throws IOException {
        long start = 0L, end = 0L;
        long inputChecksum = 0L, outputChecksum = 0L;
        int inBufferSize = 32*1024;

        System.out.println("============ testCopyFileToCacheInOneShot ====================");

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key, override),new CRC32());
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
    public void testCopyFileToCacheInOneShotOverride() throws IOException {
        long outputChecksum = testCopyFileToCacheInOneShot(true);

        //get the file from cache again
        long checksumFromCache = getFileChecksumFromCache();

        Assert.assertEquals(checksumFromCache, outputChecksum);
        Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        Assert.assertTrue(cache.getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheInOneShotAppend() throws IOException {
        long outputChecksum = testCopyFileToCacheInOneShot(false);

        //get the file from cache again
        long checksumFromCache = getFileChecksumFromCache();

        Assert.assertEquals(checksumFromCache, outputChecksum);
        Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        Assert.assertTrue(cache.getSize() > 1); // should be at least 2 (master key + chunk key)
    }

    @Test
    public void testCopyFileToCacheInOneShotOverrideMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheInOneShotOverride();
        int initialCacheSizeAfter1 = cache.getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheInOneShot(true);

            //get the file from cache again
            long checksumFromCache = getFileChecksumFromCache();

            int newCacheSize = cache.getSize();
            Assert.assertEquals(initialCacheSizeAfter1, newCacheSize);
            Assert.assertEquals(checksumFromCache, inputFileCheckSum);
        }
    }

    @Test
    public void testCopyFileToCacheInOneShotAppendMultipleTimes() throws IOException {
        int copyIterations = 5;

        testCopyFileToCacheInOneShotAppend();
        int initialCacheSizeAfter1 = cache.getSize();

        for(int i=0;i<copyIterations;i++) {
            testCopyFileToCacheInOneShot(false);

            //get the file from cache again
            long checksumFromCache = getFileChecksumFromCache();

            int newCacheSize = cache.getSize();
            Assert.assertTrue(newCacheSize > initialCacheSizeAfter1);
            Assert.assertTrue(newCacheSize == 1 + (initialCacheSizeAfter1 - 1) * (i+2));
            Assert.assertNotEquals(checksumFromCache, inputFileCheckSum);
        }
    }
}