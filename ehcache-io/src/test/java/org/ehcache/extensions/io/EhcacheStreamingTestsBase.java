package org.ehcache.extensions.io;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * Created by FabienSanglier on 5/5/15.
 */
public abstract class EhcacheStreamingTestsBase {
    public static final String ENV_CACHE_NAME = "ehcache.config.cachename";
    public static final String ENV_CACHEMGR_NAME = "ehcache.config.cachemgr.name";
    public static final String ENV_CACHE_CONFIGPATH = "ehcache.config.path";

    public static final String DEFAULT_CACHE_NAME = "FileStore";
    public static final String DEFAULT_CACHEMGR_NAME = "EhcacheStreamsTest";

    protected static final int IN_FILE_SIZE = 200 * 1024 * 1024;
    protected static final Path TESTS_DIR_PATH = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"));
    protected static final Path IN_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(),"sample_big_file_in.txt");
    protected static final Path OUT_FILE_PATH = FileSystems.getDefault().getPath(TESTS_DIR_PATH.toString(), "sample_big_file_out.txt");
    public static NumberFormat formatD = new DecimalFormat("#.###");

    protected Cache cache;
    protected static final String cache_key = "some_key";

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        generateBigFile();
    }

    private static void generateBigFile() throws Exception {
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

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        //remove files
        Files.delete(IN_FILE_PATH);
        Files.delete(OUT_FILE_PATH);
    }

    @Before
    public void setUp() throws Exception {
        CacheManager cm = getCacheManager(System.getProperty(ENV_CACHEMGR_NAME, DEFAULT_CACHEMGR_NAME), System.getProperty(ENV_CACHE_CONFIGPATH, null));
        String cacheName = System.getProperty(ENV_CACHE_NAME, DEFAULT_CACHE_NAME);
        try {
            cache = cm.getCache(cacheName);
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (ClassCastException e) {
            e.printStackTrace();
        } catch (CacheException e) {
            e.printStackTrace();
        }

        if (cache == null) {
            throw new IllegalArgumentException("Could not find the cache " + cacheName);
        }
    }

    @After
    public void tearDown() throws Exception {
        CacheManager cm = getCacheManager(System.getProperty(ENV_CACHEMGR_NAME, DEFAULT_CACHEMGR_NAME), System.getProperty(ENV_CACHE_CONFIGPATH, null));
        if(null != cm)
            cm.shutdown();
    }

    private CacheManager getCacheManager(String cacheManagerName, String resourcePath) {
        CacheManager cm = null;
        if (null == (cm = CacheManager.getCacheManager(cacheManagerName))) {
            String configLocationToLoad = null;
            if (null != resourcePath && !"".equals(resourcePath)) {
                configLocationToLoad = resourcePath;
            } else if (null != System.getProperty(ENV_CACHE_CONFIGPATH)) {
                configLocationToLoad = System.getProperty(ENV_CACHE_CONFIGPATH);
            }

            if (null != configLocationToLoad) {
                InputStream inputStream = null;
                try {
                    if (configLocationToLoad.indexOf("file:") > -1) {
                        inputStream = new FileInputStream(configLocationToLoad.substring("file:".length()));
                    } else if (configLocationToLoad.indexOf("classpath:") > -1) {
                        inputStream = this.getClass().getClassLoader().getResourceAsStream(configLocationToLoad.substring("classpath:".length()));
                    } else { //default to classpath if no prefix is specified
                        inputStream = this.getClass().getClassLoader().getResourceAsStream(configLocationToLoad);
                    }

                    if (inputStream == null) {
                        throw new FileNotFoundException("File at '" + configLocationToLoad + "' not found");
                    }

                    cm = CacheManager.create(inputStream);
                } catch (IOException ioe) {
                    throw new CacheException(ioe);
                } finally {
                    if (null != inputStream) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            throw new CacheException(e);
                        }
                        inputStream = null;
                    }
                }
            } else {
                cm = CacheManager.getInstance();
            }
        }

        return cm;
    }

    protected void pipeStreamsByteByByte(InputStream is, OutputStream os) throws IOException {
        int n;
        while ((n = is.read()) > -1) {
            os.write(n);
        }
    }

    protected void pipeStreamsWithBuffer(InputStream is, OutputStream os, int bufferSize) throws IOException {
        int n;
        byte[] buffer = new byte[bufferSize];
        while ((n = is.read(buffer)) > -1) {
            os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
        }
    }
}
