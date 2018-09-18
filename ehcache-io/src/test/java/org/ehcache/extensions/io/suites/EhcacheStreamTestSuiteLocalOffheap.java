package org.ehcache.extensions.io.suites;

import org.ehcache.extensions.io.EhcacheStreamTestSuiteBase;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public class EhcacheStreamTestSuiteLocalOffheap extends EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        EhcacheStreamTestSuiteBase.setup();
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHETEST_TYPE, EhcacheStreamingTestsBase.CacheTestType.LOCAL_OFFHEAP.getPropValue());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        EhcacheStreamTestSuiteBase.cleanup();
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHETEST_TYPE);
    }
}
