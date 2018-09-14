package org.ehcache.extensions.io;

import org.ehcache.extensions.io.impl.EhcacheStreamUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public class EhcacheStreamTestSuiteLocalHeapObjectCacheKey extends EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        System.setProperty(EhcacheStreamUtils.PROP_INPUTSTREAM_BUFFERSIZE, new Integer(inBufferSize).toString());
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH, "classpath:ehcache_localheap.xml");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME, "EhcacheStreamsTest");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME, "FileStore");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE, "object");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        System.clearProperty(EhcacheStreamUtils.PROP_INPUTSTREAM_BUFFERSIZE);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEKEY_TYPE);
    }
}
