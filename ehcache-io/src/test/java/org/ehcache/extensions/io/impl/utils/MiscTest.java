package org.ehcache.extensions.io.impl.utils;

import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public class MiscTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(MiscTest.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        logger.debug("============ Starting EhcacheStreamMiscTest ====================");
        sysPropDefaultSetup();
        cacheStart();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        sysPropDefaultCleanup();
        logger.debug("============ Finished EhcacheStreamMiscTest ====================");
    }

    @Before
    public void setup() throws Exception {
        cacheSetUp();
        printAllTestProperties();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
    }


    @Test
    public void StayOpenLong() throws Exception {
        logger.info("============ Starting testOpenReadersNoClose ====================");

        Thread.sleep(120000);
    }
}