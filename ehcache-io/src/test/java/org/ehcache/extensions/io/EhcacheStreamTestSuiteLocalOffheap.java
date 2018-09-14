package org.ehcache.extensions.io;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public class EhcacheStreamTestSuiteLocalOffheap extends EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH, "classpath:ehcache_localoffheap.xml");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME, "EhcacheStreamsOffheapTest");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME, "FileStoreOffheap");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE, "string");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE);
    }
}
