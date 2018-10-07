package org.ehcache.extensions.io.impl.utils;

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

public class EhcacheStreamCasTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamCasTest.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    final int threadCount = 10;
    final int iterations = 100;
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

    @Test
    public void testOpenReadersInThreads() throws Exception {
        logger.info("============ Starting testOpenReadersInThreads ====================");

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
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.openReadOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
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
        Assert.assertEquals(threadCount*iterations,testObjectCheck.getReaders());
    }

    @Test
    public void testOpenAndCloseReadersInDifferentThreads() throws Exception {
        logger.info("============ Starting testOpenAndCloseReadersInDifferentThreads ====================");

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
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.openReadOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
                    return null;
                }
            });
        }

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.closeReadOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
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
        Assert.assertEquals(0,testObjectCheck.getReaders());
    }

    @Test
    public void testOpenAndCloseReadersInSameThreads() throws Exception {
        logger.info("============ Starting testOpenAndCloseReadersInSameThreads ====================");

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
                    for(int i=0;i<iterations;i++) {
                        try {
                            streamUtilsInternal.openReadOnMaster(
                                    getCacheKey(),
                                    openTimeoutMillis
                            );
                        } finally {
                            streamUtilsInternal.closeReadOnMaster(
                                    getCacheKey(),
                                    openTimeoutMillis
                            );
                        }
                    }
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
        Assert.assertEquals(0,testObjectCheck.getReaders());
    }

    @Test
    public void testOpenAndCloseWritersInDifferentThreads() throws Exception {
        logger.info("============ Starting testOpenAndCloseWritersInDifferentThreads ====================");

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
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.openWriteOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
                    return null;
                }
            });
        }

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.closeWriteOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
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
        Assert.assertTrue(testObjectCheck.getLastWrittenNanos() > 0);
    }

    @Test
    public void testOpenAndCloseWritersInSameThreads() throws Exception {
        logger.info("============ Starting testOpenAndCloseWritersInSameThreads ====================");

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
                    for(int i=0;i<iterations;i++) {
                        try {
                            streamUtilsInternal.openWriteOnMaster(
                                    getCacheKey(),
                                    openTimeoutMillis
                            );
                        } finally {
                            streamUtilsInternal.closeWriteOnMaster(
                                    getCacheKey(),
                                    openTimeoutMillis
                            );
                        }
                    }
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
        Assert.assertTrue(testObjectCheck.getLastWrittenNanos() > 0);
    }

    @Test
    public void testOpenAndCloseReadersWritersInDifferentThreads() throws Exception {
        logger.info("============ Starting testOpenAndCloseReadersWritersInDifferentThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Throwable>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Throwable>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Throwable>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.openWriteOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Throwable>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.closeWriteOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Throwable>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.openReadOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Throwable>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.closeReadOnMaster(
                                getCacheKey(),
                                openTimeoutMillis
                        );
                    }
                    return null;
                }
            });


        }

        runInThreads(callables, callableResults, exceptions);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i*4).get()); // should have 0 exception
            Assert.assertNull(exceptions.get(i*4+1).get()); // should have 0 exception
            Assert.assertNull(exceptions.get(i*4+2).get()); // should have 0 exception
            Assert.assertNull(exceptions.get(i*4+3).get()); // should have 0 exception
        }

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertTrue(testObjectCheck.getLastWrittenNanos() > 0);
        Assert.assertTrue(testObjectCheck.getLastReadNanos() > 0);
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementReadersWritersInSameThreads() throws Exception {
        logger.info("============ Starting testCasLoopReplaceIncrementAndDecrementReadersWritersInSameThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Throwable>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Throwable>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Throwable>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        try {
                            streamUtilsInternal.openWriteOnMaster(
                                    getCacheKey(),
                                    openTimeoutMillis
                            );
                        } finally {
                            streamUtilsInternal.closeWriteOnMaster(
                                    getCacheKey(),
                                    openTimeoutMillis
                            );
                        }
                    }
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Throwable>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        try {
                            streamUtilsInternal.openReadOnMaster(
                                    getCacheKey(),
                                    openTimeoutMillis
                            );
                        } finally {
                            streamUtilsInternal.closeReadOnMaster(
                                    getCacheKey(),
                                    openTimeoutMillis
                            );
                        }
                    }
                    return null;
                }
            });
        }

        runInThreads(callables, callableResults, exceptions);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i*2).get()); // should have 0 exception
            Assert.assertNull(exceptions.get(i*2+1).get()); // should have 0 exception
        }

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertTrue(testObjectCheck.getLastWrittenNanos() > 0);
        Assert.assertTrue(testObjectCheck.getLastReadNanos() > 0);
    }
}