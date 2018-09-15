package org.ehcache.extensions.io.suites;

import org.ehcache.extensions.io.EhcacheStreamTestSuiteBase;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public class EhcacheStreamTestSuiteLocalOffheapObjectCacheKey extends EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        EhcacheStreamTestSuiteLocalOffheap.setup();
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE, "object");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        EhcacheStreamTestSuiteLocalOffheap.cleanup();
    }
}
