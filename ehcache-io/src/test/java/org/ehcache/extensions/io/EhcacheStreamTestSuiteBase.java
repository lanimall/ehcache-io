package org.ehcache.extensions.io;

import org.ehcache.extensions.io.impl.EhcacheInputStreamTest;
import org.ehcache.extensions.io.impl.EhcacheOutputStreamTest;
import org.ehcache.extensions.io.impl.EhcacheStreamConcurrentTest;
import org.ehcache.extensions.io.impl.EhcacheStreamUtilsTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by fabien.sanglier on 9/13/18.
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({
        EhcacheInputStreamTest.class,
        EhcacheOutputStreamTest.class,
        EhcacheStreamUtilsTest.class,
        EhcacheStreamConcurrentTest.class
})
public class EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        EhcacheStreamingTestsBase.sysPropDefaultSetup();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        EhcacheStreamingTestsBase.sysPropDefaultCleanup();
    }
}
