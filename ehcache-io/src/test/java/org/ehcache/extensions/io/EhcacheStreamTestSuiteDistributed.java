package org.ehcache.extensions.io;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public class EhcacheStreamTestSuiteDistributed extends EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH, "classpath:ehcache_distributed.xml");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME, "EhcacheStreamsDistributedTest");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME, "FileStoreDistributed");
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
