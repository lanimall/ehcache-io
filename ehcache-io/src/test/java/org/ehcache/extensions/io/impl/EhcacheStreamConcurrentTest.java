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
        System.out.println("============ Starting EhcacheStreamConcurrentTest ====================");
        cacheStart();
        generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        System.out.println("============ Finished EhcacheStreamConcurrentTest ====================");
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

    public void runInThreads(List<Callable> callables, List<AtomicInteger> exceptionCounters, boolean disableReadLocks) throws InterruptedException {
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

        try{
            System.setProperty(EhcacheStreamUtils.PROP_INPUTSTREAM_OPEN_LOCK_DISABLE, "" + new Boolean(disableReadLocks).booleanValue());

            //start the workers
            for (ThreadWorker worker : workerList) {
                worker.start();
            }
        } finally {
            System.clearProperty(EhcacheStreamUtils.PROP_INPUTSTREAM_OPEN_LOCK_DISABLE);
        }

        //wait that all operations are finished
        stopLatch.await();
    }

    class EhcacheInputStreamTestParams extends EhcacheInputStreamParams {
        final long sleepBeforeOpenMillis;
        final long sleepDuringCopyMillis;
        final long sleepAfterCopyBeforeCloseMillis;
        final long sleepAfterCloseMillis;

        EhcacheInputStreamTestParams(Ehcache cache, Object cacheKey, boolean allowNullStream, int bufferSize, long openTimeout, long sleepBeforeOpenMillis, long sleepDuringCopyMillis, long sleepAfterCopyBeforeCloseMillis, long sleepAfterCloseMillis) {
            super(cache, cacheKey, allowNullStream, bufferSize, openTimeout);
            this.sleepBeforeOpenMillis = sleepBeforeOpenMillis;
            this.sleepDuringCopyMillis = sleepDuringCopyMillis;
            this.sleepAfterCopyBeforeCloseMillis = sleepAfterCopyBeforeCloseMillis;
            this.sleepAfterCloseMillis = sleepAfterCloseMillis;
        }
    }

    class EhcacheOutputStreamTestParams extends EhcacheOuputStreamParams {
        final long sleepBeforeOpenMillis;
        final long sleepDuringCopyMillis;
        final long sleepAfterCopyBeforeCloseMillis;
        final long sleepAfterCloseMillis;

        EhcacheOutputStreamTestParams(Ehcache cache, Object cacheKey, boolean override, int bufferSize, long openTimeout, long sleepBeforeOpenMillis, long sleepDuringCopyMillis, long sleepAfterCopyBeforeCloseMillis, long sleepAfterCloseMillis) {
            super(cache, cacheKey, override, bufferSize, openTimeout);
            this.sleepBeforeOpenMillis = sleepBeforeOpenMillis;
            this.sleepDuringCopyMillis = sleepDuringCopyMillis;
            this.sleepAfterCopyBeforeCloseMillis = sleepAfterCopyBeforeCloseMillis;
            this.sleepAfterCloseMillis = sleepAfterCloseMillis;
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
                System.out.println(String.format("Thread [%s] - Before Open - Sleeping for %d millis", threadName, ehcacheInputStreamParams.sleepBeforeOpenMillis));
                TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepBeforeOpenMillis);

                try (
                        CheckedInputStream is = new CheckedInputStream(ehcacheInputStream, new CRC32());
                        CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
                ) {
                    {
                        System.out.println(String.format("Thread [%s] - Started the reading from ehcache", threadName));

                        byte[] buffer = new byte[copyBufferSize];
                        int n;
                        while ((n = is.read(buffer)) > -1) {
                            os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write

                            System.out.println(String.format("Thread [%s] - During Copy - Sleeping for %d millis", threadName, ehcacheInputStreamParams.sleepDuringCopyMillis));
                            TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepDuringCopyMillis);
                        }

                        System.out.println(String.format("Thread [%s] - Done with reading all the bytes from ehcache", threadName));

                        System.out.println(String.format("Thread [%s] - After Copy Before Close - Sleeping for %d millis", threadName, ehcacheInputStreamParams.sleepAfterCopyBeforeCloseMillis));
                        TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepAfterCopyBeforeCloseMillis);
                    }

                    System.out.println(String.format("Thread [%s] - Completely finished reading from ehcache (eg. read lock released)", threadName));

                    System.out.println(String.format("Thread [%s] - After Close - Sleeping for %d millis", threadName, ehcacheInputStreamParams.sleepAfterCloseMillis));
                    TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepAfterCloseMillis);

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
                System.out.println(String.format("Thread [%s] - Before Open - Sleeping for %d millis", threadName, ehcacheOuputStreamParams.sleepBeforeOpenMillis));
                TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepBeforeOpenMillis);

                try (
                        CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),fileReadBufferSize),new CRC32());
                        CheckedOutputStream os = new CheckedOutputStream(ehcacheOutputStream,new CRC32())
                )
                {
                    System.out.println(String.format("Thread [%s] - Started the writing to ehcache", threadName));

                    byte[] buffer = new byte[copyBufferSize];
                    int n;
                    while ((n = is.read(buffer)) > -1) {
                        os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write

                        System.out.println(String.format("Thread [%s] - During Copy - Sleeping for %d millis", threadName, ehcacheOuputStreamParams.sleepDuringCopyMillis));
                        TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepDuringCopyMillis);
                    }

                    System.out.println(String.format("Thread [%s] - Done with writing all the bytes to ehcache", threadName));

                    System.out.println(String.format("Thread [%s] - After Copy Before Close - Sleeping for %d millis", threadName, ehcacheOuputStreamParams.sleepAfterCopyBeforeCloseMillis));
                    TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepAfterCopyBeforeCloseMillis);
                }

                System.out.println(String.format("Thread [%s] - Completely finished  writing to ehcache (eg. write lock released)", threadName));

                System.out.println(String.format("Thread [%s] - After Close - Sleeping for %d millis", threadName, ehcacheOuputStreamParams.sleepAfterCloseMillis));
                TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepAfterCloseMillis);

                System.out.println(String.format("Thread [%s] - Finished callable operation", threadName));
                return null;
            }
        });

        exceptions.add(new AtomicInteger());
    }

    @Test
    public void testReadDuringWriteEnoughTime() throws EhcacheStreamException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 1000L;
        final long writerSleepBeforeOpenMillis = 0L;
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 5000L;
        final long writerSleepAfterCloseMillis = 100L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                )
        );


        final long ehcacheReadOpenTimeout = 200000L;
        final long readerSleepBeforeOpenMillis = 1000L;
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 0L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        boolean disableReadLock = false;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(0, exceptions.get(1).get()); // read thread should have 0 exception
    }

    @Test
    public void test_NoReadLock_ReadDuringWriteEnoughTime() throws EhcacheStreamException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 1000L;
        final long writerSleepBeforeOpenMillis = 0L;
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 10000L; //this is lower than non stop timeout
        final long writerSleepAfterCloseMillis = 100L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 200000L;
        final long readerSleepBeforeOpenMillis = 1000L;
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 0L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        boolean disableReadLock = true;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(0, exceptions.get(1).get()); // read thread should have 0 exception
    }

    @Test
    public void test_NoReadLock_WriteDuringReadEnoughWaitTime() throws IOException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 200000L;
        final long writerSleepBeforeOpenMillis = 500L;
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 0L;
        final long writerSleepAfterCloseMillis = 100L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 1000L;
        final long readerSleepBeforeOpenMillis = 0L;
        final long readerSleepDuringCopyMillis = 500L; //make it a lengthy read
        final long readerSleepAfterCopyBeforeCloseMillis = 0L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        boolean disableReadLock = true;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(1, exceptions.get(1).get()); // read thread should have 0 exception
    }

    @Test
    public void testWriteDuringReadEnoughWaitTime() throws IOException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 200000L;
        final long writerSleepBeforeOpenMillis = 1000L;
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 0L;
        final long writerSleepAfterCloseMillis = 100L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 1000L;
        final long readerSleepBeforeOpenMillis = 0L;
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 5000L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        boolean disableReadLock = false;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(0, exceptions.get(1).get()); // read thread should have 0 exception
    }


    @Test
    public void testMultipleReads() throws IOException, InterruptedException {
        final int threadCount = 10;
        final boolean allowNullStream = false;

        for(int i = 0; i < threadCount; i++) {
            final long ehcacheReadOpenTimeout = 1000L; //small timeout...should be enough even under high reads
            final long readerSleepBeforeOpenMillis = 0L;
            final long readerSleepDuringCopyMillis = 0L;
            final long readerSleepAfterCopyBeforeCloseMillis = 2000L;
            final long readerSleepAfterCloseMillis = 100L;

            addReadCallable(
                    new EhcacheInputStreamTestParams(
                            getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                    )
            );
        }

        boolean disableReadLock = false;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertEquals(0, exceptions.get(i).get()); // read thread should have 0 exception
        }
    }

    @Test
    public void testMultipleWritesOverwriteEnoughWaitTime() throws IOException, InterruptedException {
        final int threadCount = 10;
        final boolean override = true;

        for(int i = 0; i < threadCount; i++) {
            final long ehcacheWriteOpenTimeout = 200000L; //long enough timeout
            final long writerSleepBeforeOpenMillis = 0L;
            final long writerSleepDuringCopyMillis = 0L;
            final long writerSleepAfterCopyBeforeCloseMillis = 2000L;
            final long writerSleepAfterCloseMillis = 100L;

            addWriteCallable(
                    new EhcacheOutputStreamTestParams(
                            getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                    )
            );
        }

        boolean disableReadLock = false;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertEquals(0, exceptions.get(i).get()); // read thread should have 0 exception
        }
    }

    @Test
    public void testMultipleWritesAppendEnoughWaitTime() throws IOException, InterruptedException {
        final int threadCount = 5;
        final boolean override = false;

        for(int i = 0; i < threadCount; i++) {
            final long ehcacheWriteOpenTimeout = 200000L; //long enough timeout
            final long writerSleepBeforeOpenMillis = 0L;
            final long writerSleepDuringCopyMillis = 0L;
            final long writerSleepAfterCopyBeforeCloseMillis = 2000L;
            final long writerSleepAfterCloseMillis = 100L;

            addWriteCallable(
                    new EhcacheOutputStreamTestParams(
                            getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                    )
            );
        }

        boolean disableReadLock = false;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        for(int i = 0; i < threadCount; i++) {
            Assert.assertEquals(0, exceptions.get(i).get()); // read thread should have 0 exception
        }
    }

    @Test
    public void testReadCannotAcquireDuringWrite() throws IOException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 1000L;
        final long writerSleepBeforeOpenMillis = 0L;
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 5000L; //this is larger than the read lock
        final long writerSleepAfterCloseMillis = 100L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 2000L;
        final long readerSleepBeforeOpenMillis = 1000L;
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 0L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        boolean disableReadLock = false;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(1, exceptions.get(1).get()); // read thread should have 1 exception
    }

    @Test
    public void test_NoReadLock_ReadCannotAcquireDuringWrite() throws IOException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 1000L;
        final long writerSleepBeforeOpenMillis = 0L;
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 35000L; //this is set larger than the non-stop exception (on purpose to make sure the read fails)
        final long writerSleepAfterCloseMillis = 100L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 2000L;
        final long readerSleepBeforeOpenMillis = 1000L;
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 0L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        boolean disableReadLock = true;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        Assert.assertEquals(0, exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(1, exceptions.get(1).get()); // read thread should have 1 exception
    }

    @Test
    public void testWriteCannotAcquireDuringRead() throws IOException, InterruptedException {
        final long ehcacheWriteOpenTimeout = 2000L;
        final long writerSleepBeforeOpenMillis = 1000L;
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 0L;
        final long writerSleepAfterCloseMillis = 100L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, writerSleepBeforeOpenMillis, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis, writerSleepAfterCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 1000L;
        final long readerSleepBeforeOpenMillis = 0L;
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 5000L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        boolean disableReadLock = false;
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(exceptions), disableReadLock);

        Assert.assertEquals(1, exceptions.get(0).get()); // write thread should have 1 exception
        Assert.assertEquals(0, exceptions.get(1).get()); // read thread should have 0 exception
    }
}
