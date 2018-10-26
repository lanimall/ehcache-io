package com.softwareag.samples;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheIOStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(FileCopyCacheApp.class);
    private static final boolean isDebug = logger.isDebugEnabled();
    protected static final long SLEEP_AT_END = 60000L;

    public static NumberFormat formatD = new DecimalFormat("#.###");
    protected static final Path TESTS_DIR_PATH = FileSystems.getDefault().getPath(System.getProperty("user.home"), "TmpFileCopyCacheApp");
    protected static final String OUT_FILENAME = "sample_big_file_out";

    protected static final int BIG_FILE_SIZE = 10 * 1024 * 1024;
    protected static final int KEY_COUNT = 10;
    protected static final int KEY_EXTRA_READ_COUNT = 100;

    protected static CacheManager cm;
    protected static Ehcache cache;

    static long filePathChecksum = 0L;

    public FileCopyCacheApp() throws IOException {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String cacheMgrPath = "/ehcache.xml";
        String cacheName = "FileStoreLocal";

        boolean generateBigFile = (args.length > 0)? Boolean.parseBoolean(args[0]) : true;
        boolean cleanupBigFile = (args.length > 1)? Boolean.parseBoolean(args[1]) : false;

        Path fullFilePath = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), "sample_big_file_in.txt");
        if(generateBigFile)
            filePathChecksum = generateBigFile(BIG_FILE_SIZE, fullFilePath);
        else
            filePathChecksum = readFileFromDisk(fullFilePath);

        FileCopyCacheApp app = new FileCopyCacheApp();
        try {
            setUpCache(cacheMgrPath, cacheName);

            app.run(fullFilePath, true);

            //sleeping to give us time to look around in TMC
            Thread.sleep(SLEEP_AT_END);
        } catch (Exception exc){
            logger.error("Exception", exc);
        } finally {
            if(cleanupBigFile)
                cleanupFiles(fullFilePath);
            tearDownCache();
        }
    }

    public void run(Path inputFilePath, boolean cleanOutputFilesAtEnd) throws IOException {
        try {
            //get file from cache again to see the effect of local caching
            for(int i=0;i<KEY_COUNT;i++) {
                String cache_key = "somekey" + i;
                boolean useGzip = false;
                Path outPath = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), String.format("%s.%s.txt",OUT_FILENAME,cache_key));

                long putCheckSum = copyFileToCache(inputFilePath, cache_key, useGzip);
                //check checksums
                if (filePathChecksum != putCheckSum)
                    throw new IllegalStateException(String.format("Originial bigfile checksums [%s] and current put in cache checksums [%s] should be equal.", filePathChecksum, putCheckSum));

                for(int j=0;j<KEY_EXTRA_READ_COUNT;j++) {
                    long getChecksum = copyCacheToFile(cache_key, outPath, useGzip);

                    //check checksums
                    if (filePathChecksum != getChecksum)
                        throw new IllegalStateException(String.format("Get checksums [%s] and put checksums [%s] should be equal.", filePathChecksum, getChecksum));
                }

            }
        } catch (Exception exc){
            logger.error("Exception", exc);
        } finally {
            if(cleanOutputFilesAtEnd) {
                for (int i = 0; i < KEY_COUNT; i++) {
                    String cache_key = "somekey" + i;
                    Path outPath = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), String.format("%s.%s.txt", OUT_FILENAME, cache_key));
                    cleanupFiles(outPath);
                }
            }
        }
    }

    static void setUpCache(String cacheManagerPath, String cacheName) throws IOException {
        try {
            InputStream inputStream = FileCopyCacheApp.class.getResourceAsStream(cacheManagerPath);
            cm = CacheManager.create(inputStream);
            cache = cm.getEhcache(cacheName);
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

    static void tearDownCache() throws IOException {
        if(null != cm)
            cm.shutdown();
    }

    static void cleanupFiles(Path filePath) throws IOException {
        if(Files.exists(filePath))
            Files.delete(filePath);
    }

    long createCRC32(byte[] chunk) {
        if(null == chunk)
            throw new IllegalArgumentException("Cannot calculate checksum on null byte array");

        Checksum checksum = new CRC32();
        checksum.update(chunk, 0, chunk.length);
        long checksumValue = checksum.getValue();

        return checksumValue;
    }

    long copyFileToCache(Path inFilePath, Object cache_key, boolean useGzip) throws IOException {
        int inBufferSize = 32*1024;
        int copyBufferSize = 128*1024;
        long returnChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(inFilePath),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream((useGzip)?new GZIPOutputStream(EhcacheIOStreams.getOutputStream(cache, cache_key, true)): EhcacheIOStreams.getOutputStream(cache, cache_key, true),new CRC32())
        )
        {
            logger.info("============ Copy File To Cache " + ((useGzip) ? "(with Gzip Compression)" : "") + " ====================");

            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            returnChecksum = os.getChecksum().getValue();

            logger.info("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
            logger.info("Input Checksum = " + is.getChecksum().getValue());
            logger.info("Output Checksum = " + returnChecksum);
            logger.info("============================================");
        }
        return returnChecksum;
    }

    long copyCacheToFile(Object cache_key, Path outFilePath, boolean useGzip) throws IOException {
        int copyBufferSize = 512 * 1024; //copy buffer size
        long returnChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream((useGzip)?new GZIPInputStream(EhcacheIOStreams.getInputStream(cache, cache_key)): EhcacheIOStreams.getInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(outFilePath)), new CRC32())
        )
        {
            logger.info("============ Copy Cache To File " + ((useGzip) ? "(with Gzip Decompression)" : "") + "====================");
            long start = System.nanoTime();;
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();;

            returnChecksum = os.getChecksum().getValue();

            logger.info("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
            logger.info("Input Checksum = " + is.getChecksum().getValue());
            logger.info("Output Checksum = " + returnChecksum);
            logger.info("============================================");
        }
        return returnChecksum;
    }

    static long readFileFromDisk(Path inFilePath) throws IOException {
        int copyBufferSize = 512 * 1024; //copy buffer size
        long returnChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(inFilePath)), new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
        )
        {
            logger.debug("============ readFileFromDisk ====================");

            long start = System.nanoTime();
            pipeStreamsWithBuffer(is, os, copyBufferSize);
            long end = System.nanoTime();

            returnChecksum = os.getChecksum().getValue();

            logger.debug("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");
            logger.debug(String.format("CheckSums: %d", returnChecksum));
            logger.debug("============================================");
        }
        return returnChecksum;
    }

    static long generateBigFile(int size, String basePath, String... filePath) throws IOException {
        Path fullPath = FileSystems.getDefault().getPath(basePath, filePath);
        return generateBigFile(size, fullPath);
    }

    static long generateBigFile(int size, Path filePath) throws IOException {
        long addFileCheckSum = 0L;
        try (
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(filePath)), new CRC32());
        ) {
            logger.info("============ Generate Initial Big File ====================");
            logger.info("Path: " + filePath);

            byte[] buffer = new byte[10 * 1024];
            int len = buffer.length;

            long start = System.nanoTime();;
            int byteCount = 0;
            for (int i = 0; i < size; i++) {
                if(byteCount == len || i == size - 1){
                    os.write(buffer, 0, byteCount);
                    byteCount=0;
                }

                buffer[byteCount]=new Integer(i).byteValue();
                byteCount++;
            }
            long end = System.nanoTime();

            addFileCheckSum = os.getChecksum().getValue();

            logger.info("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
            logger.info("CheckSum = " + addFileCheckSum);
            logger.info("============================================");
        }
        return addFileCheckSum;
    }

    static void pipeStreamsByteByByte(InputStream is, OutputStream os) throws IOException {
        int n;
        while ((n = is.read()) > -1) {
            os.write(n);
        }
    }

    static void pipeStreamsWithBuffer(InputStream is, OutputStream os, int bufferSize) throws IOException {
        int n;
        byte[] buffer = new byte[bufferSize];
        while ((n = is.read(buffer)) > -1) {
            os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
        }
    }
}
