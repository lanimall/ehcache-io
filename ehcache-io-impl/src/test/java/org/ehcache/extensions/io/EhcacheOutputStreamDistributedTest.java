package org.ehcache.extensions.io;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by FabienSanglier on 5/5/15.
 */
public class EhcacheOutputStreamDistributedTest extends EhcacheOutputStreamTest {
    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty(ENV_CACHE_CONFIGPATH, "classpath:ehcache-distributed.xml");
        System.setProperty(ENV_CACHEMGR_NAME, "EhcacheStreamsDistributedTest");
        System.setProperty(ENV_CACHE_NAME, "FileStore-Distributed");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        System.clearProperty(ENV_CACHE_CONFIGPATH);
        System.clearProperty(ENV_CACHEMGR_NAME);
        System.clearProperty(ENV_CACHE_NAME);
    }
}
