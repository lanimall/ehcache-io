package org.ehcache.extensions.io.impl.utils;

import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.cas.ExponentialWait;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public class EhcacheStreamCasTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamCasTest.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    final int threadCount = 100;
    final long openTimeoutMillis = 60000;

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

    private void openAndCloseWriter(final EhcacheStreamUtilsInternal streamUtilsInternal) throws EhcacheStreamTimeoutException {
        EhcacheStreamMaster ehcacheStreamMaster = null;
        try {
            ehcacheStreamMaster = streamUtilsInternal.openWriteOnMaster(
                    getCacheKey(),
                    openTimeoutMillis
            );
        } finally {
            if(null != ehcacheStreamMaster) {
                streamUtilsInternal.closeWriteOnMaster(
                        getCacheKey(),
                        openTimeoutMillis
                );
            }
        }
    }

    private void openAndCloseReader(final EhcacheStreamUtilsInternal streamUtilsInternal) throws EhcacheStreamTimeoutException {
        EhcacheStreamMaster ehcacheStreamMaster = null;
        try {
            ehcacheStreamMaster = streamUtilsInternal.openReadOnMaster(
                    getCacheKey(),
                    openTimeoutMillis
            );
        } finally {
            if(null != ehcacheStreamMaster) {
                streamUtilsInternal.closeReadOnMaster(
                        getCacheKey(),
                        openTimeoutMillis
                );
            }
        }
    }

    @Test
    public void testOpenReadersNoClose() throws Exception {
        logger.info("============ Starting testOpenReadersNoClose ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Throwable>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Throwable>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        //first, let's open and close a writer to make sure there's a master entry in cache
        try {
            streamUtilsInternal.openWriteOnMaster(getCacheKey(), openTimeoutMillis);
        } finally {
            streamUtilsInternal.closeWriteOnMaster(getCacheKey(), openTimeoutMillis);
        }

        Assert.assertEquals(1, getCache().getSize()); // should be 1 now

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> resultInc = new AtomicReference<Long>();
            final AtomicReference<Throwable> exceptionInc = new AtomicReference<Throwable>();

            callableResults.add(resultInc);
            exceptions.add(exceptionInc);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    streamUtilsInternal.openReadOnMaster(
                            getCacheKey(),
                            openTimeoutMillis
                    );
                    return null;
                }
            });
        }

        runInThreads(callables, callableResults, exceptions);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // should have 0 exception
        }

        Assert.assertEquals(1, getCache().getSize()); // should still be 1 now

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(threadCount,testObjectCheck.getReaders());
    }

    @Test
    public void testOpenAndCloseReadersNonNullEntry() throws Exception {
        logger.info("============ Starting testOpenAndCloseReadersNonNullEntry ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Throwable>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Throwable>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        //add and close a writer so there's a cached entry first
        openAndCloseWriter(streamUtilsInternal);

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    openAndCloseReader(streamUtilsInternal);
                    return null;
                }
            });
        }

        runInThreads(callables, callableResults, exceptions);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // should have 0 exception
        }

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertNotNull(testObjectCheck);
        Assert.assertEquals(0,testObjectCheck.getReaders());
    }


    @Test
    public void testOpenAndCloseReadersNullEntry() throws Exception {
        logger.info("============ Starting testOpenAndCloseReadersNullEntry ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Throwable>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Throwable>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    openAndCloseReader(streamUtilsInternal);
                    return null;
                }
            });
        }

        runInThreads(callables, callableResults, exceptions);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // should have 0 exception
        }

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertNull(testObjectCheck);
    }

    @Test
    public void testOpenAndCloseWriters() throws Exception {
        logger.info("============ Starting testOpenAndCloseWriters ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Throwable>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Throwable>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    openAndCloseWriter(streamUtilsInternal);
                    return null;
                }
            });
        }

        runInThreads(callables, callableResults, exceptions);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // should have 0 exception
        }

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertTrue(testObjectCheck.getLastWrittenTime() > 0);
    }

    @Test
    public void testOpenAndCloseReadersWriters() throws Exception {
        logger.info("============ Starting testOpenAndCloseReadersWriters ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Throwable>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Throwable>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        //adding an initial write entry to avoid having all the reads end with null returns before the writes even start
        openAndCloseWriter(streamUtilsInternal);

        for(int i = 0; i < threadCount; i++) {
            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Throwable>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    openAndCloseWriter(streamUtilsInternal);
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Throwable>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    openAndCloseReader(streamUtilsInternal);
                    return null;
                }
            });
        }

        runInThreads(callables, callableResults, exceptions);

        if(isDebug) {
            for(int i = 0; i < threadCount; i++) {
                logger.debug("Exception: {}", EhcacheStreamUtilsInternal.toStringSafe(exceptions.get(i * 2).get()));
                logger.debug("Exception: {}", EhcacheStreamUtilsInternal.toStringSafe(exceptions.get(i * 2 + 1).get()));
            }
        }

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i*2).get()); // should have 0 exception
            Assert.assertNull(exceptions.get(i*2+1).get()); // should have 0 exception
        }

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertTrue(testObjectCheck.getLastWrittenTime() > 0);
        Assert.assertTrue(testObjectCheck.getLastReadTime() > 0);
    }
}