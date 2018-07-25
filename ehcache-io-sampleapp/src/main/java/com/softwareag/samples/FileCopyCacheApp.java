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
import java.util.zip.*;

/**
 * Created by FabienSanglier on 5/7/15.
 */
public class FileCopyCacheApp {
    public static NumberFormat formatD = new DecimalFormat("#.###");
    protected static final int IN_FILE_SIZE = 10 * 1024 * 1024; //
    protected static final Path TESTS_DIR_PATH = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"));
    protected static final Path IN_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(),"sample_big_file_in.txt");
    protected static final String OUT_FILENAME = "sample_big_file_out";
    protected static final int KEY_COUNT = 10;
    protected static final int KEY_EXTRA_READ_COUNT = 100;

    private CacheManager cm;
    private Cache cache;

    public FileCopyCacheApp() throws IOException {
    }

    public void run(String cacheMgrPath, String cacheName, boolean cleanFilesAtEnd) throws IOException {
        try {
            setUpCache(cacheMgrPath, cacheName);
            generateBigFile();

            //get file from cache again to see the effect of local caching
            for(int i=0;i<KEY_COUNT;i++) {
                String cache_key = "somekey" + i;
                boolean useGzip = false;
                Path outPath = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), String.format("%s.%s.txt",OUT_FILENAME,cache_key));
                copyFileToCache(IN_FILE_PATH, cache_key, useGzip);
                copyCacheToFile(cache_key, outPath, useGzip);

                for(int j=0;j<KEY_EXTRA_READ_COUNT;j++) {
                    copyCacheToFile(cache_key, outPath, useGzip);
                }
            }
        } catch (Exception exc){
            System.out.println("Exception:" + exc.getMessage());
            exc.printStackTrace();
        } finally {
            tearDownCache();

            if(cleanFilesAtEnd) {
                cleanupFiles(IN_FILE_PATH);
                for (int i = 0; i < KEY_COUNT; i++) {
                    String cache_key = "somekey" + i;
                    Path outPath = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), String.format("%s.%s.txt", OUT_FILENAME, cache_key));
                    cleanupFiles(outPath);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String cacheMgrPath = "ehcache.xml";
        String cacheName = "FileStore";

        FileCopyCacheApp app = new FileCopyCacheApp();
        app.run(cacheMgrPath, cacheName, false);
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

    void cleanupFiles(Path filePath) throws IOException {
        if(Files.exists(filePath))
            Files.delete(filePath);
    }

    public Checksum copyFileToCache(Path inFilePath, Object cache_key, boolean useGzip) throws IOException {
        int inBufferSize = 32*1024;
        int copyBufferSize = 128*1024;
        Checksum returnChecksum = null;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(inFilePath),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream((useGzip)?new GZIPOutputStream(new EhcacheOutputStream(cache, cache_key)):new EhcacheOutputStream(cache, cache_key),new CRC32())
        )
        {
            System.out.println("============ Copy File To Cache " + ((useGzip)?"(with Gzip Compression)":"") + " ====================");

            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            returnChecksum = os.getChecksum();

            System.out.println("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            System.out.println("Input Checksum = " + is.getChecksum().getValue());
            System.out.println("Output Checksum = " + returnChecksum.getValue());
            System.out.println("============================================");
        }
        return returnChecksum;
    }

    public Checksum copyCacheToFile(Object cache_key, Path outFilePath, boolean useGzip) throws IOException {
        int copyBufferSize = 512 * 1024; //copy buffer size
        Checksum returnChecksum = null;

        try (
                CheckedInputStream is = new CheckedInputStream((useGzip)?new GZIPInputStream(new EhcacheInputStream(cache, cache_key)):new EhcacheInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(outFilePath)), new CRC32())
        )
        {
            System.out.println("============ Copy Cache To File " + ((useGzip)?"(with Gzip Decompression)":"") + "====================");
            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            returnChecksum = os.getChecksum();

            System.out.println("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
            System.out.println("Input Checksum = " + is.getChecksum().getValue());
            System.out.println("Output Checksum = " + returnChecksum.getValue());
            System.out.println("============================================");
        }
        return returnChecksum;
    }

    void generateBigFile() throws IOException {
        try (
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(IN_FILE_PATH)), new CRC32());
        ) {
            System.out.println("============ Generate Initial Big File ====================");
            System.out.println("Path: " + IN_FILE_PATH);

            byte[] buffer = new byte[10 * 1024];
            int len = buffer.length;

            long start = System.nanoTime();;
            int byteCount = 0;
            for (int i = 0; i < IN_FILE_SIZE; i++) {
                if(byteCount == len || i == IN_FILE_SIZE - 1){
                    os.write(buffer, 0, byteCount);
                    byteCount=0;
                }

                buffer[byteCount]=new Integer(i).byteValue();
                byteCount++;
            }
            long end = System.nanoTime();

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
