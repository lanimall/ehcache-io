package org.ehcache.extensions.io;

import org.ehcache.extensions.io.impl.EhcacheInputStreamTest;
import org.ehcache.extensions.io.impl.EhcacheOutputStreamTest;
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
        EhcacheOutputStreamTest.class
        })
public class EhcacheStreamTestSuite {
    @BeforeClass
    public static void setup() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        System.setProperty(EhcacheIOStreams.PROP_INPUTSTREAM_BUFFERSIZE, new Integer(inBufferSize).toString());
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH, "classpath:ehcache_localheap.xml");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME, "EhcacheStreamsTest");
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME, "FileStore");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        int inBufferSize = 128 * 1024; //ehcache input stream internal buffer
        System.clearProperty(EhcacheIOStreams.PROP_INPUTSTREAM_BUFFERSIZE);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_CONFIGPATH);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHEMGR_NAME);
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHE_NAME);
    }
}
