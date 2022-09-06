package com.softwareag.samples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 * Created by FabienSanglier on 5/8/15.
 */
public class FileCopyCacheDistributedApp extends FileCopyCacheApp {
    private static final Logger logger = LoggerFactory.getLogger(FileCopyCacheDistributedApp.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    public FileCopyCacheDistributedApp() throws IOException {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String cacheMgrPath = "/ehcache-distributed.xml";
        String cacheName = "FileStoreDistributed";

        boolean generateBigFile = (args.length > 0)? Boolean.parseBoolean(args[0]) : false;
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
}