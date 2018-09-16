package org.ehcache.extensions.io.suites;

import org.ehcache.extensions.io.EhcacheStreamTestSuiteBase;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public class EhcacheStreamTestSuiteLocalHeap extends EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        EhcacheStreamTestSuiteBase.setup();

        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH, "classpath:ehcache_localheap.xml");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME, "EhcacheStreamsTest");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME, "FileStore");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        EhcacheStreamTestSuiteBase.cleanup();

        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME);
    }
}
