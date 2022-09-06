package com.softwareag.samples;

import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Created by FabienSanglier on 5/8/15.
 */
public class FileCopyDecoratedCacheApp extends FileCopyCacheApp {
    private static final Logger logger = LoggerFactory.getLogger(FileCopyDecoratedCacheApp.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    public FileCopyDecoratedCacheApp() throws IOException {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String cacheMgrPath = "/ehcache-distributed-decorated.xml";
        String cacheName = "FileStoreDistributed";

        boolean generateBigFile = (args.length > 0)? Boolean.parseBoolean(args[0]) : false;
        boolean cleanupBigFile = (args.length > 1)? Boolean.parseBoolean(args[1]) : false;

        Path fullFilePath = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), "sample_big_file_in.txt");
        if(generateBigFile)
            filePathChecksum = generateBigFile(BIG_FILE_SIZE, fullFilePath);
        else
            filePathChecksum = readFileFromDisk(fullFilePath);

        FileCopyDecoratedCacheApp app = new FileCopyDecoratedCacheApp();
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

    public void run(Path fullFilePath, boolean cleanFilesAtEnd) throws IOException {
        try {
            //get file from cache again to see the effect of local caching
            for(int i=0;i<KEY_COUNT;i++) {
                String cache_key = "somekey" + i;
                long putCheckSum = 0L;

                for(int j=0;j<KEY_EXTRA_READ_COUNT;j++) {
                    byte[] fileContent = getFileFromCache(cache_key);
                    if (null == fileContent) {
                        logger.info("Key {} not in cache - will add it", cache_key);
                        putCheckSum = putFileInCache(fullFilePath, cache_key);

                        //check checksums
                        if (filePathChecksum != putCheckSum)
                            throw new IllegalStateException(String.format("Originial bigfile checksums [%s] and current put in cache checksums [%s] should be equal.", filePathChecksum, putCheckSum));

                        fileContent = getFileFromCache(cache_key);
                    } else {
                        logger.info("Key {} is in cache, great!", cache_key);
                    }

                    //check checksums
                    long getChecksum = createCRC32(fileContent);
                    if (filePathChecksum != getChecksum)
                        throw new IllegalStateException(String.format("Get checksums [%s] and put checksums [%s] should be equal.", filePathChecksum, getChecksum));
                }
            }
        } catch (Exception exc){
            logger.error("Exception", exc);
        } finally {
            if(cleanFilesAtEnd) {
                cleanupFiles(fullFilePath);
                for (int i = 0; i < KEY_COUNT; i++) {
                    String cache_key = "somekey" + i;
                    Path outPath = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), String.format("%s.%s.txt", OUT_FILENAME, cache_key));
                    cleanupFiles(outPath);
                }
            }
        }
    }

    public long putFileInCache(Path inFilePath, Object cache_key) throws IOException {
        int inBufferSize = 32*1024;
        long returnChecksum = 0L;

        try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(inFilePath),inBufferSize),new CRC32());
        )
        {
            logger.info("============ Put key {} in decorated cache ====================", cache_key);

            long start = System.nanoTime();

            cache.put(new Element(cache_key, is));

            long end = System.nanoTime();

            returnChecksum = is.getChecksum().getValue();

            logger.info("Execution Time = " + formatD.format((double) (end - start) / 1000000) + " millis");
        }
        return returnChecksum;
    }

    public byte[] getFileFromCache(Object cache_key) throws IOException {
        logger.info("============ Get key {} from decorated cache ====================", cache_key);

        byte[] returnedValue = null;

        long start = System.nanoTime();

        Element element = cache.get(cache_key);
        if(null == element)
            returnedValue = null;
        else
            returnedValue = (byte[])element.getObjectValue();

        long end = System.nanoTime();

        logger.info("Execution Time = " + formatD.format((double)(end - start) / 1000000) + " millis");

        return returnedValue;
    }
}