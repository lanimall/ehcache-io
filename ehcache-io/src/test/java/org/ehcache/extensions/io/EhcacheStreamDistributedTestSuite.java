package org.ehcache.extensions.io;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by FabienSanglier on 5/6/15.
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({
        EhcacheInputStreamDistributedTest.class,
        EhcacheOutputStreamDistributedTest.class
        })
public class EhcacheStreamDistributedTestSuite {
}
