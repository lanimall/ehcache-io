package org.ehcache.extensions.io;

import org.junit.Assert;
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
    @Test
    public void testCopyFileToFile() throws Exception {
        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH)), new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32())
        )
        {
            System.out.println("============ testCopyFileToFile ====================");

            int copyBufferSize = 32*1024;

            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void testCopyFileToCacheByteByByte() throws IOException {
        int inBufferSize = 32*1024;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key),new CRC32())
        )
        {
            System.out.println("============ testCopyFileToCacheByteByByte ====================");

            long start = System.nanoTime();;
            pipeStreamsByteByByte(is, os);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void testCopyFileToCacheWithBuffer() throws IOException {
        int inBufferSize = 32*1024;
        int copyBufferSize = 128*1024;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key),new CRC32())
        )
        {
            System.out.println("============ testCopyFileToCacheWithBuffer ====================");

            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }

    @Test
    public void testCopyFileToCacheInOneShot() throws IOException {
        int inBufferSize = 32*1024;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key),new CRC32());
                ByteArrayOutputStream bos = new ByteArrayOutputStream(new Double(IN_FILE_SIZE * 1.2).intValue())
        )
        {
            System.out.println("============ testCopyFileToCacheInOneShot ====================");

            //first stream from file to ByteArrayOutputStream
            pipeStreamsByteByByte(is, bos);
            byte[] fullBytes = bos.toByteArray();

            long start = System.nanoTime();;
            os.write(fullBytes);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");

            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
        }
    }
}