package org.ehcache.extensions.io.suites;

import org.ehcache.extensions.io.EhcacheStreamTestSuiteBase;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public class EhcacheStreamTestSuiteDistributedObjectCacheKey extends EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        EhcacheStreamTestSuiteDistributed.setup();
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE, "object");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        EhcacheStreamTestSuiteDistributed.cleanup();
    }
}
