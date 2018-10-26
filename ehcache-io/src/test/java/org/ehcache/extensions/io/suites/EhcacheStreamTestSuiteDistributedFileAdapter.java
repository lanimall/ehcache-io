package org.ehcache.extensions.io.suites;

import org.ehcache.extensions.io.EhcacheStreamTestSuiteBase;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.FileSystems;

/**
 * Created by FabienSanglier on 5/6/15.
 */

public class EhcacheStreamTestSuiteDistributedFileAdapter extends EhcacheStreamTestSuiteBase {
    @BeforeClass
    public static void setup() throws Exception {
        EhcacheStreamTestSuiteBase.setup();
        System.setProperty(EhcacheStreamingTestsBase.ENV_CACHETEST_TYPE, EhcacheStreamingTestsBase.CacheTestType.CLUSTERED_STRONG.getPropValue());

        System.setProperty(PropertyUtils.PROP_INPUTSTREAM_FILEADAPTER_ENABLED, new Boolean(true).toString());
        System.setProperty(PropertyUtils.PROP_INPUTSTREAM_FILEADAPTER_PATH, System.getProperty("user.home") + "/" + "ehcache-io-temp");
        System.setProperty(PropertyUtils.PROP_INPUTSTREAM_FILEADAPTER_THREASHOLD_SIZE, "0");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        EhcacheStreamTestSuiteBase.cleanup();
        System.clearProperty(EhcacheStreamingTestsBase.ENV_CACHETEST_TYPE);

        System.clearProperty(PropertyUtils.PROP_INPUTSTREAM_FILEADAPTER_ENABLED);
        System.clearProperty(PropertyUtils.PROP_INPUTSTREAM_FILEADAPTER_PATH);
        System.clearProperty(PropertyUtils.PROP_INPUTSTREAM_FILEADAPTER_THREASHOLD_SIZE);
    }
}
