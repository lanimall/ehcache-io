package com.softwareag.samples;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import org.ehcache.extensions.io.EhcacheInputStream;
import org.ehcache.extensions.io.EhcacheOutputStream;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Created by FabienSanglier on 5/7/15.
 */
public class FileCopyCacheApp {
    public static NumberFormat formatD = new DecimalFormat("#.###");
    protected static final int IN_FILE_SIZE = 200 * 1024 * 1024;
    protected static final Path TESTS_DIR_PATH = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"));
    protected static final Path IN_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(),"sample_big_file_in.txt");
    protected static final Path OUT_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), "sample_big_file_out.txt");

    private CacheManager cm;
    private Cache cache;

    public FileCopyCacheApp() throws IOException {
    }

    public static void main(String[] args) throws IOException {
        String cacheMgrPath = "ehcache.xml";
        String cacheName = "FileStore";

        FileCopyCacheApp app = new FileCopyCacheApp();

        try {
            app.setUpCache(cacheMgrPath, cacheName);
            app.generateBigFile();

            String cache_key = "somekey";
            app.copyFileToCache(IN_FILE_PATH, cache_key);
            app.copyCacheToFile(cache_key, OUT_FILE_PATH);
        } finally {
            app.tearDownCache();
            app.cleanupFiles(IN_FILE_PATH, OUT_FILE_PATH);
        }
    }

    void setUpCache(String cacheManagerPath, String cacheName) throws IOException {
        try {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(cacheManagerPath);
            this.cm = CacheManager.create(inputStream);
            this.cache = cm.getCache(cacheName);
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (ClassCastException e) {
            e.printStackTrace();
        } catch (CacheException e) {
            e.printStackTrace();
        }

        if (cache == null) {
            throw new IllegalArgumentException("Could not find the cache " + cacheName + " in " + cacheManagerPath);
        }
    }

    void tearDownCache() throws IOException {
        if(null != cm)
            cm.shutdown();
    }

    void cleanupFiles(Path inFilePath, Path outFilePath) throws IOException {
        //remove files
        Files.delete(inFilePath);
        Files.delete(outFilePath);
    }

    public void copyFileToCache(Path inFilePath, Object cache_key) throws IOException {
        int inBufferSize = 32*1024;
        int copyBufferSize = 128*1024;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(inFilePath),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key),new CRC32())
        )
        {
            System.out.println("============ testCopyFileToCacheWithBuffer ====================");

            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("============================================");
        }
    }

    public void copyCacheToFile(Object cache_key, Path outFilePath) throws IOException {
        int copyBufferSize = 512 * 1024; //copy buffer size

        try (
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(outFilePath)), new CRC32())
        )
        {
            System.out.println("============ copyCacheToFileUsingStreamDefaultBuffers ====================");
            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
            System.out.println("============================================");
        }
    }

    void generateBigFile() throws IOException {
        try (
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(IN_FILE_PATH)), new CRC32());
        ) {
            System.out.println("============ Generate Initial Big File ====================");

            long start = System.nanoTime();;
            int size = IN_FILE_SIZE;
            for (int i = 0; i < size; i++) {
                os.write(i);
            }
            long end = System.nanoTime();;

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("CheckSum = " + os.getChecksum().getValue());
            System.out.println("============================================");
        }
    }

    void pipeStreamsByteByByte(InputStream is, OutputStream os) throws IOException {
        int n;
        while ((n = is.read()) > -1) {
            os.write(n);
        }
    }

    void pipeStreamsWithBuffer(InputStream is, OutputStream os, int bufferSize) throws IOException {
        int n;
        byte[] buffer = new byte[bufferSize];
        while ((n = is.read(buffer)) > -1) {
            os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
        }
    }
}
