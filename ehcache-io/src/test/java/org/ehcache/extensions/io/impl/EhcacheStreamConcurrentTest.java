package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Created by fabien.sanglier on 9/12/18.
 */
public class EhcacheStreamConcurrentTest extends EhcacheStreamingTestsBase {
    private List<Callable> callables;
    private List<AtomicInteger> exceptions;

    final int ehcacheWriteBufferSize = 32*1024;
    final int ehcacheReadBufferSize = 32*1024;
    private final int copyBufferSize = 128*1024;
    private final int fileReadBufferSize = 32*1024;

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        cacheStart();
        generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
    }

    @Before
    public void setup() throws Exception {
        callables = new ArrayList<Callable>();
        exceptions = new ArrayList<AtomicInteger>();
        cacheSetUp();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        callables = null;
        exceptions = null;
    }

    public class ThreadWorker extends Thread {
        private final Callable callable;
        private final CountDownLatch doneLatch;
        private AtomicInteger exceptionCounter;

        public ThreadWorker(Callable callable, CountDownLatch doneLatch, AtomicInteger exceptionCounter) {
            super();
            this.callable = callable;
            this.doneLatch = doneLatch;
            this.exceptionCounter = exceptionCounter;
        }

        @Override
        public void run() {
            String threadName = getName();

            try {
                callable.call();
            } catch (EhcacheStreamException e) {
                if(null != exceptionCounter)
                    exceptionCounter.incrementAndGet();

                System.out.println(String.format("Thread [%s] - %s", getName(), e.getMessage()));
            } catch (Exception e) {
                if(null != exceptionCounter)
                    exceptionCounter.incrementAndGet();

                System.out.println(String.format("Thread [%s] - %s", getName(), e.getMessage()));
                throw new IllegalStateException("Something unexpected happened", e);
            } finally{
                doneLatch.countDown();
            }
        }
    }

    public void runInThreads(List<Callable> callables, List<AtomicInteger> exceptionCounters) throws InterruptedException {
        if(null == callables)
            throw new IllegalStateException("must provides some operations to run...");

        if(null != exceptionCounters && callables.size() != exceptionCounters.size())
            throw new IllegalStateException("must provides the same number of exception counters as the number of callables");

        final List<ThreadWorker> workerList = new ArrayList<>(callables.size());
        final CountDownLatch stopLatch = new CountDownLatch(callables.size());

        //add first worker
        int count = 0;
        for(int i = 0; i < callables.size(); i++) {
            workerList.add(new ThreadWorker(callables.get(i), stopLatch, exceptionCounters.get(i)));
        }

        //start the workers
        for (ThreadWorker worker : workerList) {
            worker.start();
        }

        //wait that all operations are finished
        stopLatch.await();
    }

    class EhcacheInputStreamTestParams extends EhcacheInputStreamParams {
        final long sleepBeforeMillis;
        final long sleepDuringMillis;
        final long sleepAfterMillis;

        EhcacheInputStreamTestParams(Ehcache cache, Object cacheKey, boolean allowNullStream, int bufferSize, long openTimeout, long sleepBeforeMillis, long sleepDuringMillis, long sleepAfterMillis) {
            super(cache, cacheKey, allowNullStream, bufferSize, openTimeout);
            this.sleepBeforeMillis = sleepBeforeMillis;
            this.sleepDuringMillis = sleepDuringMillis;
            this.sleepAfterMillis = sleepAfterMillis;
        }
    }

    class EhcacheOutputStreamTestParams extends EhcacheOuputStreamParams {
        final long sleepBeforeMillis;
        final long sleepDuringMillis;
        final long sleepAfterMillis;

        EhcacheOutputStreamTestParams(Ehcache cache, Object cacheKey, boolean override, int bufferSize, long openTimeout, long sleepBeforeMillis, long sleepDuringMillis, long sleepAfterMillis) {
            super(cache, cacheKey, override, bufferSize, openTimeout);
            this.sleepBeforeMillis = sleepBeforeMillis;
            this.sleepDuringMillis = sleepDuringMillis;
            this.sleepAfterMillis = sleepAfterMillis;
        }
    }

    private void addReadCallable(final EhcacheInputStreamTestParams ehcacheInputStreamParams) throws EhcacheStreamException {
        callables.add(new Callable() {
            @Override
            public Long call() throws Exception {
                final InputStream ehcacheInputStream = EhcacheIOStreams.getInputStream(
                        ehcacheInputStreamParams.cache,
                        ehcacheInputStreamParams.cacheKey,
                        ehcacheInputStreamParams.allowNullStream,
                        ehcacheInputStreamParams.bufferSize,
                        ehcacheInputStreamParams.openTimeout);

                Thread.currentThread().setName("Ehcache-Reader-Thread-"+ Thread.currentThread().getId());

                String threadName = Thread.currentThread().getName();
                System.out.println(String.format("Thread [%s] - Before - Sleeping for %d millis", threadName, ehcacheInputStreamParams.sleepBeforeMillis));
                TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepBeforeMillis);

                try (
                        CheckedInputStream is = new CheckedInputStream(ehcacheInputStream, new CRC32());
                        CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
                ) {
                    {
                        System.out.println(String.format("Thread [%s] - Started the reading from ehcache", threadName));

                        pipeStreamsWithBuffer(is, os, copyBufferSize);

                        System.out.println(String.format("Thread [%s] - Done with reading all the bytes from ehcache", threadName));

                        System.out.println(String.format("Thread [%s] - During - Sleeping for %d millis", threadName, ehcacheInputStreamParams.sleepDuringMillis));
                        TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepDuringMillis);
                    }

                    System.out.println(String.format("Thread [%s] - Completely finished reading from ehcache (eg. read lock released)", threadName));

                    System.out.println(String.format("Thread [%s] - After - Sleeping for %d millis", threadName, ehcacheInputStreamParams.sleepAfterMillis));
                    TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepAfterMillis);

                    System.out.println(String.format("Thread [%s] - Finished callable operation", threadName));
                    return null;
                }
            }
        });

        exceptions.add(new AtomicInteger());
    }

    private void addWriteCallable(final EhcacheOutputStreamTestParams ehcacheOuputStreamParams) throws EhcacheStreamException {
        callables.add(new Callable() {
            @Override
            public Long call() throws Exception {
                final OutputStream ehcacheOutputStream = EhcacheIOStreams.getOutputStream(
                        ehcacheOuputStreamParams.cache,
                        ehcacheOuputStreamParams.cacheKey,
                        ehcacheOuputStreamParams.override,
                        ehcacheOuputStreamParams.bufferSize,
                        ehcacheOuputStreamParams.openTimeout);

                Thread.currentThread().setName("Ehcache-Writer-Thread-"+ Thread.currentThread().getId());

                String threadName = Thread.currentThread().getName();
                System.out.println(String.format("Thread [%s] - Before - Sleeping for %d millis", threadName, ehcacheOuputStreamParams.sleepBeforeMillis));
                TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepBeforeMillis);

                try (
                        CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),fileReadBufferSize),new CRC32());
                        CheckedOutputStream os = new CheckedOutputStream(ehcacheOutputStream,new CRC32())
                )
                {
                    System.out.println(String.format("Thread [%s] - Started the writing to ehcache", threadName));

                    pipeStreamsWithBuffer(is, os, copyBufferSize);

                    System.out.println(String.format("Thread [%s] - Done with writing all the bytes to ehcache", threadName));

                    System.out.println(String.format("Thread [%s] - During - Sleeping for %d millis", threadName, ehcacheOuputStreamParams.sleepDuringMillis));
                    TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepDuringMillis);
                }

                System.out.println(String.format("Thread [%s] - Completely finished  writing to ehcache (eg. write lock released)", threadName));

                System.out.println(String.format("Thread [%s] - After - Sleeping for %d millis", threadName, ehcacheOuputStreamParams.sleepAfterMillis));
                TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepAfterMillis);

                System.out.println(String.format("Thread [%s] - Finished callable operation", threadName));
                return null;
            }
        });

        exceptions.add(new AtomicInteger());
    }

    @Test
    public void testReadDuringWriteEnoughTime() throws EhcacheStreamException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 1000L;
        final long writerSleepBeforeMillis = 0L;
        final long writerSleepDuringMillis = 10000L;
        final long writerSleepAfterMillis = 100L;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), true, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeMillis, writerSleepDuringMillis, writerSleepAfterMillis
                )
        );

        final long ehcacheReadOpenTimeout = 200000L;
        final long readerSleepBeforeMillis = 5000L;
        final long readerSleepDuringMillis = 0L;
        final long readerSleepAfterMillis = 100L;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), false, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeMillis, readerSleepDuringMillis, readerSleepAfterMillis
                )
        );

        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions));

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(0, exceptions.get(1).get()); // read thread should have 0 exception
    }

    @Test
    public void testWriteDuringReadEnoughWaitTime() throws IOException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 200000L;
        final long writerSleepBeforeMillis = 5000L;
        final long writerSleepDuringMillis = 0L;
        final long writerSleepAfterMillis = 100L;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), true, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeMillis, writerSleepDuringMillis, writerSleepAfterMillis
                )
        );

        final long ehcacheReadOpenTimeout = 1000L;
        final long readerSleepBeforeMillis = 0L;
        final long readerSleepDuringMillis = 10000L;
        final long readerSleepAfterMillis = 100L;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), false, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeMillis, readerSleepDuringMillis, readerSleepAfterMillis
                )
        );

        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions));

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(0, exceptions.get(1).get()); // read thread should have 0 exception
    }

    @Test
    public void testMultipleReadsNoProblem() throws IOException, InterruptedException {
        int threadCount = 10;

        for(int i = 0; i < threadCount; i++) {
            final long ehcacheReadOpenTimeout = 1000L; //small timeout...should be enough even under high reads
            final long readerSleepBeforeMillis = 0L;
            final long readerSleepDuringMillis = 10000L;
            final long readerSleepAfterMillis = 100L;

            addReadCallable(
                    new EhcacheInputStreamTestParams(
                            getCache(), getCacheKey(), false, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeMillis, readerSleepDuringMillis, readerSleepAfterMillis
                    )
            );
        }

        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions));

        for(int i = 0; i < threadCount; i++) {
            Assert.assertEquals(0, exceptions.get(i).get()); // read thread should have 0 exception
        }
    }

    @Test
    public void testMultipleWritesEnoughWaitTime() throws IOException, InterruptedException {
        int threadCount = 10;

        for(int i = 0; i < threadCount; i++) {
            final long ehcacheWriteOpenTimeout = 200000L; //long enough timeout
            final long writerSleepBeforeMillis = 0L;
            final long writerSleepDuringMillis = 5000L;
            final long writerSleepAfterMillis = 100L;

            addWriteCallable(
                    new EhcacheOutputStreamTestParams(
                            getCache(), getCacheKey(), true, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeMillis, writerSleepDuringMillis, writerSleepAfterMillis
                    )
            );
        }

        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions));

        for(int i = 0; i < threadCount; i++) {
            Assert.assertEquals(0, exceptions.get(i).get()); // read thread should have 0 exception
        }
    }

    @Test
    public void testReadCannotAcquireDuringWrite() throws IOException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 1000L;
        final long writerSleepBeforeMillis = 0L;
        final long writerSleepDuringMillis = 10000L;
        final long writerSleepAfterMillis = 100L;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), true, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeMillis, writerSleepDuringMillis, writerSleepAfterMillis
                )
        );

        final long ehcacheReadOpenTimeout = 2000L;
        final long readerSleepBeforeMillis = 2000L;
        final long readerSleepDuringMillis = 0L;
        final long readerSleepAfterMillis = 100L;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), false, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeMillis, readerSleepDuringMillis, readerSleepAfterMillis
                )
        );

        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions));

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(1, exceptions.get(1).get()); // read thread should have 1 exception
    }

    @Test
    public void testWriteCannotAcquireDuringRead() throws IOException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 5000L;
        final long writerSleepBeforeMillis = 2000L;
        final long writerSleepDuringMillis = 0L;
        final long writerSleepAfterMillis = 100L;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), true, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeMillis, writerSleepDuringMillis, writerSleepAfterMillis
                )
        );

        final long ehcacheReadOpenTimeout = 1000L;
        final long readerSleepBeforeMillis = 0L;
        final long readerSleepDuringMillis = 10000L;
        final long readerSleepAfterMillis = 100L;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), false, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeMillis, readerSleepDuringMillis, readerSleepAfterMillis
                )
        );

        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions));

        Assert.assertEquals(1, exceptions.get(0).get()); // write thread should have 1 exception
        Assert.assertEquals(0, exceptions.get(1).get()); // read thread should have 0 exception
    }
}
