package org.ehcache.extensions.io;

import org.ehcache.extensions.io.impl.EhcacheInputStreamTest;
import org.ehcache.extensions.io.impl.EhcacheOutputStreamTest;
import org.ehcache.extensions.io.impl.EhcacheStreamUtilsTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by FabienSanglier on 5/6/15.
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({
        EhcacheInputStreamTest.class,
        EhcacheOutputStreamTest.class,
        EhcacheStreamUtilsTest.class
        })
public class EhcacheStreamTestSuiteDistributedObjectCacheKey {
    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH, "classpath:ehcache_distributed.xml");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME, "EhcacheStreamsDistributedTest");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME, "FileStoreDistributed");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE, "object");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE);
    }
}