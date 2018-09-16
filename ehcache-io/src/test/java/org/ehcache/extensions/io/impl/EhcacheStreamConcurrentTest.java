package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.*;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Created by fabien.sanglier on 9/12/18.
 */
@RunWith(Parameterized.class)
public class EhcacheStreamConcurrentTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamConcurrentTest.class);

    private static long NULL_CHECKSUM = -1;
    private List<ConcurrentTestCallable> callables;
    private List<AtomicReference<Long>> callableResults;
    private List<AtomicReference<Class>> exceptions;
    private long inputFileCheckSum = NULL_CHECKSUM;

    private final int copyBufferSize = 128*1024;
    private final int fileReadBufferSize = 128*1024;

    private final int ehcacheWriteBufferSize = PropertyUtils.getOutputStreamBufferSize();
    private final int ehcacheReadBufferSize = PropertyUtils.getInputStreamBufferSize();

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        logger.info("============ Starting EhcacheStreamConcurrentTest ====================");
        sysPropDefaultSetup();
        cacheStart();
        generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        sysPropDefaultCleanup();
        logger.info("============ Finished EhcacheStreamConcurrentTest ====================");
    }

    @Before
    public void setup() throws Exception {
        setupParameterizedProperties();
        inputFileCheckSum = readFileFromDisk();
        callables = new ArrayList<ConcurrentTestCallable>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Class>>();
        cacheSetUp();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        callables = null;
        exceptions = null;
        inputFileCheckSum = NULL_CHECKSUM;
        cleanupParameterizedProperties();
    }

    public class ThreadWorker extends Thread {
        private final ConcurrentTestCallable callable;
        private final CountDownLatch doneLatch;
        private AtomicReference<Long> callableResult;
        private AtomicReference<Class> exception;

        public ThreadWorker(ConcurrentTestCallable callable, CountDownLatch doneLatch, AtomicReference<Long> callableResult, AtomicReference<Class> exception) {
            super();
            this.callable = callable;
            this.doneLatch = doneLatch;
            this.callableResult = callableResult;
            this.exception = exception;
        }

        @Override
        public void run() {
            String threadName = getName();

            try {
                callableResult.set(callable.call());
            } catch (Exception e) {
                if(null != exception)
                    exception.set(e.getClass());
                callableResult.set(NULL_CHECKSUM);
                logger.error(e.getMessage(),e);
            } finally{
                doneLatch.countDown();
            }
        }
    }

    public void runInThreads() throws InterruptedException {
        //Collections.unmodifiableList(callables), Collections.unmodifiableList(callableResults), Collections.unmodifiableList(exceptions)

        if(null == callables)
            throw new IllegalStateException("must provides some operations to run...");

        if(null != exceptions && callables.size() != exceptions.size())
            throw new IllegalStateException("must provides the same number of exception counters as the number of callables");

        final List<ThreadWorker> workerList = new ArrayList<>(callables.size());
        final CountDownLatch stopLatch = new CountDownLatch(callables.size());

        //add first worker
        int count = 0;
        for(int i = 0; i < callables.size(); i++) {
            workerList.add(new ThreadWorker(callables.get(i), stopLatch, callableResults.get(i), exceptions.get(i)));
        }

        try{
            //start the workers
            for (ThreadWorker worker : workerList) {
                worker.start();
            }
        } finally {

        }

        //wait that all operations are finished
        stopLatch.await();
    }

    abstract class ConcurrentTestCallable implements Callable<Long>{}

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
        callables.add(new ConcurrentTestCallable() {
            @Override
            public Long call() throws Exception {
                long returnChecksum = -1L;

                Thread.currentThread().setName("Ehcache-Reader-Thread-"+ Thread.currentThread().getId());

                logger.info("Before Open - Sleeping for {} millis", ehcacheInputStreamParams.sleepBeforeOpenMillis);
                TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepBeforeOpenMillis);

                try (
                        final InputStream ehcacheInputStream = EhcacheIOStreams.getInputStream(
                                ehcacheInputStreamParams.cache,
                                ehcacheInputStreamParams.cacheKey,
                                ehcacheInputStreamParams.allowNullStream,
                                ehcacheInputStreamParams.bufferSize,
                                ehcacheInputStreamParams.openTimeout);

                        CheckedInputStream is = new CheckedInputStream(ehcacheInputStream, new CRC32());
                        CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32())
                ) {
                    logger.info("Started the reading from ehcache");

                    byte[] buffer = new byte[copyBufferSize];
                    int n;
                    while ((n = is.read(buffer)) > -1) {
                        os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write

                        logger.trace("During Copy - Sleeping for {} millis", ehcacheInputStreamParams.sleepDuringCopyMillis);
                        TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepDuringCopyMillis);
                    }

                    logger.info("Done with reading all the bytes from ehcache");

                    logger.info("After Copy Before Close - Sleeping for {} millis", ehcacheInputStreamParams.sleepAfterCopyBeforeCloseMillis);
                    TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepAfterCopyBeforeCloseMillis);

                    returnChecksum = is.getChecksum().getValue();
                }

                logger.info("Completely finished writing to ehcache (stream closed)");

                logger.info("After Close - Sleeping for {} millis", ehcacheInputStreamParams.sleepAfterCloseMillis);
                TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepAfterCloseMillis);

                logger.info("Finished callable operation");

                return returnChecksum;
            }
        });

        callableResults.add(new AtomicReference<Long>());
        exceptions.add(new AtomicReference<Class>());
    }

    private void addWriteCallable(final EhcacheOutputStreamTestParams ehcacheOuputStreamParams) throws EhcacheStreamException {
        callables.add(new ConcurrentTestCallable() {
            @Override
            public Long call() throws Exception {
                long returnChecksum = -1L;

                Thread.currentThread().setName("Ehcache-Writer-Thread-"+ Thread.currentThread().getId());

                logger.info("Before Open - Sleeping for {} millis", ehcacheOuputStreamParams.sleepBeforeOpenMillis);
                TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepBeforeOpenMillis);

                try (
                        final OutputStream ehcacheOutputStream = EhcacheIOStreams.getOutputStream(
                                ehcacheOuputStreamParams.cache,
                                ehcacheOuputStreamParams.cacheKey,
                                ehcacheOuputStreamParams.override,
                                ehcacheOuputStreamParams.bufferSize,
                                ehcacheOuputStreamParams.openTimeout);

                        CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),fileReadBufferSize),new CRC32());
                        CheckedOutputStream os = new CheckedOutputStream(ehcacheOutputStream,new CRC32())
                )
                {
                    logger.info("Started the writing to ehcache");

                    byte[] buffer = new byte[copyBufferSize];
                    int n;
                    while ((n = is.read(buffer)) > -1) {
                        os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write

                        logger.trace("During Copy - Sleeping for {} millis", ehcacheOuputStreamParams.sleepDuringCopyMillis);
                        TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepDuringCopyMillis);
                    }

                    logger.info("Done with writing all the bytes to ehcache");

                    logger.info("After Copy Before Close - Sleeping for {} millis", ehcacheOuputStreamParams.sleepAfterCopyBeforeCloseMillis);
                    TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepAfterCopyBeforeCloseMillis);

                    returnChecksum = os.getChecksum().getValue();
                }

                logger.info("Completely finished writing to ehcache (stream closed)");

                logger.info("After Close - Sleeping for {} millis", ehcacheOuputStreamParams.sleepAfterCloseMillis);
                TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepAfterCloseMillis);

                logger.info("Finished callable operation");
                return returnChecksum;
            }
        });

        callableResults.add(new AtomicReference<Long>());
        exceptions.add(new AtomicReference<Class>());
    }

    @Test
    public void testReadDuringWriteEnoughTime() throws EhcacheStreamException, InterruptedException {
        logger.info("============ testReadDuringWriteEnoughTime ====================");

        final long ehcacheWriteOpenTimeout = 1000L;
        final long writerSleepBeforeOpenMillis = 0L;
        final long writerSleepDuringCopyMillis = 100L;
        final long writerSleepAfterCopyBeforeCloseMillis = 2000L;
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

        runInThreads();

        Assert.assertNull(exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertNull(exceptions.get(1).get()); // read thread should have 0 exception

        Assert.assertEquals(inputFileCheckSum, callableResults.get(0).get().longValue());
        Assert.assertEquals(inputFileCheckSum, callableResults.get(1).get().longValue());
    }

    @Test
    public void testWriteDuringReadEnoughWaitTime() throws IOException, InterruptedException {
        logger.info("============ testWriteDuringReadEnoughWaitTime ====================");

        //first, let's add something to cache so the read can start reading something
        long checksumCacheAdded = copyFileToCache(getCacheKey());
        Assert.assertEquals(inputFileCheckSum, checksumCacheAdded);

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
        final long readerSleepDuringCopyMillis = 100L; //slow down the read to create a problem in priority write mode
        final long readerSleepAfterCopyBeforeCloseMillis = 5000L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        runInThreads();

        if(PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS) {
            Assert.assertNull(exceptions.get(0).get()); // write thread should have 0 exception
            Assert.assertNull(exceptions.get(1).get()); // read thread should have 0 exception

            Assert.assertEquals(inputFileCheckSum, callableResults.get(0).get().longValue());
            Assert.assertEquals(inputFileCheckSum, callableResults.get(1).get().longValue());
        } else if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY_NOLOCK){
            Assert.assertNull(exceptions.get(0).get()); // write thread should have 0 exception

            // read thread should have EhcacheStreamConcurrentException because the write took over
            Assert.assertEquals(EhcacheStreamConcurrentException.class, exceptions.get(1).get());

            Assert.assertEquals(inputFileCheckSum, callableResults.get(0).get().longValue());
            Assert.assertEquals(NULL_CHECKSUM, callableResults.get(1).get().longValue());
        }
    }


    @Test
    public void testMultipleConcurrentReads() throws IOException, InterruptedException {
        logger.info("============ testMultipleReads ====================");

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, let's add something to cache so the read can start reading something
        long checksumCacheAdded = copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)
        Assert.assertEquals(inputFileCheckSum, checksumCacheAdded);

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

        runInThreads();

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // read thread should have 0 exception
            Assert.assertEquals(inputFileCheckSum, callableResults.get(i).get().longValue());
        }
    }

    @Test
    public void testMultipleWritesOverwriteEnoughWaitTime() throws IOException, InterruptedException {
        logger.info("============ testMultipleWritesOverwriteEnoughWaitTime ====================");

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final int threadCount = 10;
        final boolean override = true;

        //first, let's do this serially to get an expected checksum
        for(int i = 0; i < threadCount; i++) {
            copyFileToCache(getCacheKey(), override);
        }
        int expectedCacheSize = getCache().getSize();
        Assert.assertTrue(expectedCacheSize > 1); // should be at least 2 (master key + chunk key)

        //cleanup and wait a bit just to be safe
        getCache().removeAll();
        Thread.sleep(1000);
        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

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

        runInThreads();

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // read thread should have 0 exception
            Assert.assertEquals(inputFileCheckSum, callableResults.get(i).get().longValue());
        }
        Assert.assertEquals(expectedCacheSize, getCache().getSize());
    }

    @Test
    public void testMultipleWritesAppendEnoughWaitTime() throws IOException, InterruptedException {
        logger.info("============ testMultipleWritesAppendEnoughWaitTime ====================");

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final int threadCount = 5;
        final boolean override = false;

        //first, let's do this serially to get an expected checksum
        for(int i = 0; i < threadCount; i++) {
            copyFileToCache(getCacheKey(), override);
        }

        int expectedCacheSize = getCache().getSize();
        Assert.assertTrue(expectedCacheSize > 1); // should be at least 2 (master key + chunk key)

        //cleanup and wait a bit just to be safe
        getCache().removeAll();
        Thread.sleep(1000);
        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //now, run in threads
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

        runInThreads();

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // should have 0 exception
            Assert.assertEquals(inputFileCheckSum, callableResults.get(i).get().longValue());
        }
        Assert.assertEquals(expectedCacheSize, getCache().getSize());
    }

    @Test
    public void testReadDuringWrite_ReadTimeoutReached() throws IOException, InterruptedException {
        logger.info("============ testReadCannotAcquireDuringWrite ====================");

        final long ehcacheWriteOpenTimeout = 1000L;
        final long writerSleepBeforeOpenMillis = 0L; //ensured write thread starts before read thread
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
        final long readerSleepBeforeOpenMillis = 1000L; //ensured read thread starts after the write one
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 0L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        runInThreads();

        Assert.assertNull(exceptions.get(0).get()); // write thread should have 0 exception
        Assert.assertEquals(inputFileCheckSum, callableResults.get(0).get().longValue());

        Assert.assertEquals(EhcacheStreamTimeoutException.class, exceptions.get(1).get()); // read thread should have 1 exception
        Assert.assertEquals(NULL_CHECKSUM, callableResults.get(1).get().longValue());
    }

    @Test
    public void testWriteDuringRead_WriteTimeoutReached() throws IOException, InterruptedException {
        logger.info("============ testWriteCannotAcquireDuringRead ====================");
        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, let's add something to cache so the read can start reading something
        long checksumCacheAdded = copyFileToCache(getCacheKey());
        int expectedCacheSize = getCache().getSize();

        Assert.assertTrue(expectedCacheSize > 1); // should be at least 2 (master key + chunk key)
        Assert.assertEquals(inputFileCheckSum, checksumCacheAdded);

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
        final long readerSleepDuringCopyMillis = 500L;
        final long readerSleepAfterCopyBeforeCloseMillis = 5000L;
        final long readerSleepAfterCloseMillis = 100L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, readerSleepBeforeOpenMillis, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis, readerSleepAfterCloseMillis
                )
        );

        runInThreads();

        if(PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS) {
            Assert.assertEquals(EhcacheStreamTimeoutException.class, exceptions.get(0).get()); // write thread should have 1 exception
            Assert.assertNull(exceptions.get(1).get()); // read thread should have 0 exception

            Assert.assertEquals(NULL_CHECKSUM, callableResults.get(0).get().longValue());
            Assert.assertEquals(inputFileCheckSum, callableResults.get(1).get().longValue());
        } else if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY_NOLOCK){
            Assert.assertNull(exceptions.get(0).get()); // write thread should have 0 exception

            // read thread should have EhcacheStreamConcurrentException because the write took over
            Assert.assertEquals(EhcacheStreamConcurrentException.class, exceptions.get(1).get());

            Assert.assertEquals(checksumCacheAdded, callableResults.get(0).get().longValue());
            Assert.assertEquals(NULL_CHECKSUM, callableResults.get(1).get().longValue());
        }
        Assert.assertEquals(expectedCacheSize, getCache().getSize()); // should be 0 now
    }
}
