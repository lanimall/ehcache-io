package com.softwareag.samples;

import java.io.IOException;

/**
 * Created by FabienSanglier on 5/8/15.
 */
public class FileCopyCacheDistributedApp extends FileCopyCacheApp {

    public FileCopyCacheDistributedApp() throws IOException {
    }

    public static void main(String[] args) throws IOException {
        String cacheMgrPath = "ehcache-distributed.xml";
        String cacheName = "FileStore-Distributed";

        FileCopyCacheApp app = new FileCopyCacheApp();

        try {
            app.setUpCache(cacheMgrPath, cacheName);
            app.generateBigFile();

            String cache_key = "somenewkey";
            boolean useGzip = false;
            app.copyFileToCache(IN_FILE_PATH, cache_key, useGzip);
            app.copyCacheToFile(cache_key, OUT_FILE_PATH, useGzip);
            app.copyCacheToFile(cache_key, OUT_FILE_PATH, useGzip);

            cache_key = "somenewotherkey";
            useGzip = true;
            app.copyFileToCache(IN_FILE_PATH, cache_key, useGzip);
            app.copyCacheToFile(cache_key, OUT_FILE_PATH, useGzip);

            //get file from cache again to see the effect of local caching
            app.copyCacheToFile(cache_key, OUT_FILE_PATH, useGzip);
        } finally {
            app.tearDownCache();
            app.cleanupFiles(IN_FILE_PATH, OUT_FILE_PATH);
        }
    }
}