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
        app.run(cacheMgrPath, cacheName, true);
    }
}