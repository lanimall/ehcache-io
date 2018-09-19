package org.ehcache.extensions.io.impl;

import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
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

    final int threadCount = 30;
    final int iterations = 100;
    final long openTimeoutMillis = 20000;

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
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
    }

    @Test
    public void testCasLoopReplaceIncrementsReadersInThreads() throws Exception {
        logger.info("============ Starting testCasLoopReplaceIncrementsReadersInThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final boolean override = false;

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> resultInc = new AtomicReference<Long>();
            final AtomicReference<Class> exceptionInc = new AtomicReference<Class>();

            callableResults.add(resultInc);
            exceptions.add(exceptionInc);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.INCREMENT
                        );                    }
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
        logger.debug("Final EhcacheStreamMaster check from cache: {}", testObjectCheck.toString());
        Assert.assertEquals(threadCount*iterations,testObjectCheck.getReaders());
    }

    @Test
    public void testCasLoopReplaceIncrementsWritersInThreads() throws Exception {
        logger.info("============ Starting testCasLoopReplaceIncrementsWritersInThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final boolean override = false;

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> resultInc = new AtomicReference<Long>();
            final AtomicReference<Class> exceptionInc = new AtomicReference<Class>();

            callableResults.add(resultInc);
            exceptions.add(exceptionInc);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for (int i = 0; i < iterations; i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_READER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.INCREMENT
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
        logger.debug("Final EhcacheStreamMaster check from cache: {}", testObjectCheck.toString());
        Assert.assertEquals(threadCount*iterations,testObjectCheck.getWriters());
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementReadersInDifferentThreads() throws Exception {
        logger.info("============ Starting testCasLoopReplaceIncrementAndDecrementReadersInDifferentThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final boolean override = false;

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Class> exception = new AtomicReference<Class>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                        );                    }
                    return null;
                }
            });
        }

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Class> exception = new AtomicReference<Class>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
                        );                    }
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
        logger.debug("Final EhcacheStreamMaster check from cache: {}", testObjectCheck.toString());
        Assert.assertEquals(0,testObjectCheck.getReaders());
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementReadersInSameThreads() throws Exception {
        logger.info("============ Starting testCasLoopReplaceIncrementAndDecrementReadersInSameThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final boolean override = false;

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Class> exception = new AtomicReference<Class>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                        );
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
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
        logger.debug("Final EhcacheStreamMaster check from cache: {}", testObjectCheck.toString());
        Assert.assertEquals(0,testObjectCheck.getReaders());
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementWritersInDifferentThreads() throws Exception {
        logger.info("============ Starting testCasLoopReplaceIncrementAndDecrementWritersInDifferentThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final boolean override = false;

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Class> exception = new AtomicReference<Class>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                        );                    }
                    return null;
                }
            });
        }

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Class> exception = new AtomicReference<Class>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.SINGLE_WRITER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
                        );                    }
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
        logger.debug("Final EhcacheStreamMaster check from cache: {}", testObjectCheck.toString());
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertTrue(testObjectCheck.getLastWrittenNanos() > 0);
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementWritersInSameThreads() throws Exception {
        logger.info("============ Starting testCasLoopReplaceIncrementAndDecrementWritersInSameThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final boolean override = false;

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> result = new AtomicReference<Long>();
            final AtomicReference<Class> exception = new AtomicReference<Class>();

            callableResults.add(result);
            exceptions.add(exception);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                        );
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.SINGLE_WRITER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
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
        logger.debug("Final EhcacheStreamMaster check from cache: {}", testObjectCheck.toString());
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertTrue(testObjectCheck.getLastWrittenNanos() > 0);
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementReadersWritersInDifferentThreads() throws Exception {
        logger.info("============ Starting testCasLoopReplaceIncrementAndDecrementReadersWritersInDifferentThreads ====================");

        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final boolean override = false;

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Class>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                        );
                    }
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Class>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.SINGLE_WRITER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
                        );
                    }
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Class>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                        );
                    }
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Class>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
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
        logger.debug("Final EhcacheStreamMaster check from cache: {}", testObjectCheck.toString());
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
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final boolean override = false;

        final EhcacheStreamUtilsInternal streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());

        for(int i = 0; i < threadCount; i++) {
            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Class>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_READER_NO_WRITER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                        );
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.SINGLE_WRITER,
                                EhcacheStreamMaster.MutationField.WRITERS,
                                EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
                        );
                    }
                    return null;
                }
            });

            callableResults.add(new AtomicReference<Long>());
            exceptions.add(new AtomicReference<Class>());
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.INCREMENT_MARK_NOW
                        );
                        streamUtilsInternal.atomicMutateEhcacheStreamMasterInCache(
                                getCacheKey(),
                                openTimeoutMillis,
                                override,
                                EhcacheStreamMaster.ComparatorType.NO_WRITER,
                                EhcacheStreamMaster.MutationField.READERS,
                                EhcacheStreamMaster.MutationType.DECREMENT_MARK_NOW
                        );
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
        logger.debug("Final EhcacheStreamMaster check from cache: {}", testObjectCheck.toString());
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertTrue(testObjectCheck.getLastWrittenNanos() > 0);
        Assert.assertTrue(testObjectCheck.getLastReadNanos() > 0);
    }
}