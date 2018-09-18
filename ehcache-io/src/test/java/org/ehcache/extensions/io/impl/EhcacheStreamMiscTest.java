package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Element;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
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

public class EhcacheStreamMiscTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamMiscTest.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    final int threadCount = 50;
    final int iterations = 100;

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

    public void testCasLoopReplaceReaderRaw(final boolean incrementMode) throws Exception {
        long t1 = System.currentTimeMillis();
        long t2 = t1; //this ensures that the while always happen at least once!
        boolean isOpen = false;
        long openTimeoutMillis = 20000;
        long itcounter = 0;

        EhcacheStreamMaster testObjectModified;
        while (!isOpen && t2 - t1 <= openTimeoutMillis) {
            Element testObjectCacheElement = getCache().get(getCacheKey());
            EhcacheStreamMaster testObject = null;
            if (null == testObjectCacheElement || null != (testObject = (EhcacheStreamMaster) testObjectCacheElement.getObjectValue()) && testObject.getWriters() == 0) {
                if (null != testObject) {
                    testObjectModified = EhcacheStreamMaster.deepCopy(testObject);
                } else {
                    testObjectModified = new EhcacheStreamMaster();
                }

                //set the master object with a new reader
                if(incrementMode) testObjectModified.addReader();
                else testObjectModified.removeReader();

                if (isDebug)
                    logger.trace("testObjectModified before CAS cache: {}", testObjectModified.toString());

                //concurrency check with CAS: let's save the initial EhcacheStreamMaster in cache, while making sure it hasn't change so far
                //if multiple threads are trying to do this replace on same key, only one thread is guaranteed to succeed here...while others will fail their CAS ops...and spin back to try again later.
                boolean replaced;
                if (testObject == null) {
                    replaced = (null == getCache().putIfAbsent(new Element(getCacheKey(), testObjectModified)));
                } else {
                    replaced = getCache().replace(testObjectCacheElement, new Element(getCacheKey(), testObjectModified));
                }

                if (replaced) {
                    if (isDebug)
                        logger.debug("cas replace successful...");

                    if (isDebug)
                        logger.debug("testObjectModified AFTER CAS replace: {}", testObjectModified.toString());

                    EhcacheStreamMaster testObjectCheck = testObjectModified.clone();

                    if (isDebug)
                        logger.debug("testObjectModified check from cache: {}", testObjectCheck.toString());

                    if (isDebug)
                        logger.debug("(testObjectModified check from cache).equals(testObjectModified): {}", testObjectCheck.equals(testObjectModified));

                    if(!testObjectModified.equals(testObjectCheck))
                        throw new EhcacheStreamIllegalStateException("(testObjectModified check from cache).equals(testObjectModified) should be TRUE");

                    //at this point, it's really open with consistency in cache
                    isOpen = true;
                    if (isDebug)
                        logger.debug("open successful...");
                } else {
                    if (isTrace)
                        logger.trace("Could not cas replace the Stream master in cache, got beat by another thread. Let's retry in a bit.");
                }
            }

            t2 = System.currentTimeMillis();
            itcounter++;
        }

        if(isDebug)
            logger.debug("Total cas loop iterations: {}", itcounter - 1);

        //if it's not opened at the end of all the tries and timeout, throw timeout exception
        if (!isOpen) {
            throw new EhcacheStreamTimeoutException(String.format("Could not acquire a read after trying for %d ms (timeout triggers at %d ms)", t2 - t1, openTimeoutMillis));
        };
    }

    @Test
    public void testCasLoopReplaceIncrementsInThreadsRaw() throws Exception {
        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> resultInc = new AtomicReference<Long>();
            final AtomicReference<Class> exceptionInc = new AtomicReference<Class>();

            callableResults.add(resultInc);
            exceptions.add(exceptionInc);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        testCasLoopReplaceReaderRaw(true);
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
        Element testObjectCheckFromCacheElement = getCache().get(getCacheKey());
        EhcacheStreamMaster testObjectCheck = (EhcacheStreamMaster)testObjectCheckFromCacheElement.getObjectValue();

        logger.debug("Final EhcacheStreamMaster check from cache -- Total count at the end: {}", testObjectCheck.getReaders());

        Assert.assertEquals(threadCount*iterations,testObjectCheck.getReaders());
    }

    @Test
    public void testCasLoopReplaceIncrementsReadersInThreadsStreamUtils() throws Exception {
        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        final long openTimeoutMillis = 20000;
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
        logger.debug("Final EhcacheStreamMaster check from cache -- Total count at the end: {}", testObjectCheck.getReaders());
        Assert.assertEquals(threadCount*iterations,testObjectCheck.getReaders());
    }

    @Test
    public void testCasLoopReplaceIncrementsWritersInThreadsStreamUtils() throws Exception {
        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        final long openTimeoutMillis = 20000;
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
                                EhcacheStreamMaster.ComparatorType.NO_READER,
                                EhcacheStreamMaster.MutationField.WRITERS,
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
        logger.debug("Final EhcacheStreamMaster check from cache -- Total count at the end: {}", testObjectCheck.getReaders());
        Assert.assertEquals(threadCount*iterations,testObjectCheck.getWriters());
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementReadersInThreadsRaw() throws Exception {
        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> resultInc = new AtomicReference<Long>();
            final AtomicReference<Class> exceptionInc = new AtomicReference<Class>();

            callableResults.add(resultInc);
            exceptions.add(exceptionInc);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for (int i = 0; i < iterations; i++) {
                        testCasLoopReplaceReaderRaw(true);
                    }
                    return null;
                }
            });

        }

        for(int i = 0; i < threadCount; i++) {
            final AtomicReference<Long> resultDec = new AtomicReference<Long>();
            final AtomicReference<Class> exceptionDec = new AtomicReference<Class>();

            callableResults.add(resultDec);
            exceptions.add(exceptionDec);
            callables.add(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    for(int i=0;i<iterations;i++) {
                        testCasLoopReplaceReaderRaw(false);
                    }
                    return null;
                }
            });
        }

        runInThreads(callables, callableResults, exceptions);

        for(int i = 0; i < callables.size(); i++) {
            Assert.assertNull(exceptions.get(i).get()); // should have 0 exception
        }

        // check the final counter
        Element testObjectCheckFromCacheElement = getCache().get(getCacheKey());
        EhcacheStreamMaster testObjectCheck = (EhcacheStreamMaster)testObjectCheckFromCacheElement.getObjectValue();
        logger.debug("Final EhcacheStreamMaster check from cache -- Total count at the end: {}", testObjectCheck.getReaders());
        Assert.assertEquals(0,testObjectCheck.getReaders());
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementReadersInThreadsStreamUtils() throws Exception {
        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        final long openTimeoutMillis = 20000;
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
        logger.debug("Final EhcacheStreamMaster check from cache -- Total count at the end: {}", testObjectCheck.getReaders());
        Assert.assertEquals(0,testObjectCheck.getReaders());
    }

    @Test
    public void testCasLoopReplaceIncrementAndDecrementWritersInThreadsStreamUtils() throws Exception {
        List<Callable<Long>> callables;
        List<AtomicReference<Long>> callableResults;
        List<AtomicReference<Class>> exceptions;

        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();

        final long openTimeoutMillis = 20000;
        final boolean override = false;
        final int threadCount = 10;
        final int iterations = 10;

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
        logger.debug("Final EhcacheStreamMaster check from cache -- Total count at the end: {}", testObjectCheck.getReaders());
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertTrue(testObjectCheck.getLastWrittenNanos() > 0);
    }
}