package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.*;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Created by fabien.sanglier on 9/12/18.
 */

//TODO: need to add some remove tests into the mix of tests here!!!
//TODO:  for example, let;'s test read / write / delete at the same time and make sure no exception
//TODO:  or delete during read --> make sure ok

@RunWith(Parameterized.class)
public class EhcacheStreamConcurrentTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamConcurrentTest.class);

    private static StreamCopyResultDescriptor bigInputFileDescriptor = null;
    private EhcacheStreamUtilsInternal streamUtilsInternal;

    private ArrayList<Callable<Long>> callables;
    private List<AtomicReference<Long>> callableResults;
    private List<AtomicReference<Throwable>> exceptions;
    private StreamCopyResultDescriptor fileFromDisk = null;

    private final int copyBufferSize = 128*1024;
    private final int fileReadBufferSize = 128*1024;

    private final int ehcacheWriteBufferSize = PropertyUtils.getOutputStreamBufferSize();
    private final int ehcacheReadBufferSize = PropertyUtils.getInputStreamBufferSize();

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        logger.debug("============ Starting EhcacheStreamConcurrentTest ====================");
        sysPropDefaultSetup();
        cacheStart();
        bigInputFileDescriptor = generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        sysPropDefaultCleanup();
        bigInputFileDescriptor = null;
        logger.debug("============ Finished EhcacheStreamConcurrentTest ====================");
    }

    @Before
    public void setup() throws Exception {
        setupParameterizedProperties();
        fileFromDisk = readFileFromDisk();
        callables = new ArrayList<Callable<Long>>();
        callableResults = new ArrayList<AtomicReference<Long>>();
        exceptions = new ArrayList<AtomicReference<Throwable>>();
        cacheSetUp();
        streamUtilsInternal = new EhcacheStreamUtilsInternal(getCache());
        printAllTestProperties();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        callables = null;
        exceptions = null;
        fileFromDisk = null;
        cleanupParameterizedProperties();
        streamUtilsInternal = null;
    }

    public void runInThreads() throws InterruptedException {
        runInThreads(Collections.unmodifiableList(callables), Collections.unmodifiableList(callableResults), Collections.unmodifiableList(exceptions));
    }

    abstract class ConcurrentTestCallable implements Callable<Long>{}

    class EhcacheInputStreamTestParams extends EhcacheInputStreamParams {
        final Synchronizer beforeOpenOrderedSync;
        final int beforeOpenOrderIndex;
        final long sleepDuringCopyMillis;
        final long sleepAfterCopyBeforeCloseMillis;

        EhcacheInputStreamTestParams(Ehcache cache, Object cacheKey, boolean allowNullStream, int bufferSize, long openTimeout, final Synchronizer beforeOpenOrderedSync, final int beforeOpenOrderIndex, long sleepDuringCopyMillis, long sleepAfterCopyBeforeCloseMillis) {
            super(cache, cacheKey, allowNullStream, bufferSize, openTimeout);
            this.beforeOpenOrderIndex = beforeOpenOrderIndex;
            this.beforeOpenOrderedSync = beforeOpenOrderedSync;
            this.sleepDuringCopyMillis = sleepDuringCopyMillis;
            this.sleepAfterCopyBeforeCloseMillis = sleepAfterCopyBeforeCloseMillis;
        }
    }

    class EhcacheOutputStreamTestParams extends EhcacheOuputStreamParams {
        final Synchronizer beforeOpenOrderedSync;
        final int beforeOpenOrderIndex;
        final long sleepDuringCopyMillis;
        final long sleepAfterCopyBeforeCloseMillis;

        EhcacheOutputStreamTestParams(Ehcache cache, Object cacheKey, boolean override, int bufferSize, long openTimeout, final Synchronizer beforeOpenOrderedSync, final int beforeOpenOrderIndex, long sleepDuringCopyMillis, long sleepAfterCopyBeforeCloseMillis) {
            super(cache, cacheKey, override, bufferSize, openTimeout);
            this.beforeOpenOrderIndex = beforeOpenOrderIndex;
            this.beforeOpenOrderedSync = beforeOpenOrderedSync;
            this.sleepDuringCopyMillis = sleepDuringCopyMillis;
            this.sleepAfterCopyBeforeCloseMillis = sleepAfterCopyBeforeCloseMillis;
        }
    }

    private void addReadCallable(final EhcacheInputStreamTestParams ehcacheInputStreamParams) throws EhcacheStreamException {
        callables.add(new ConcurrentTestCallable() {
            @Override
            public Long call() throws Exception {
                long returnChecksum = -1L;

                Thread.currentThread().setName("Ehcache-Reader-Thread-"+ Thread.currentThread().getId());

                logger.debug("Before Open");
                ehcacheInputStreamParams.beforeOpenOrderedSync.barrier(ehcacheInputStreamParams.beforeOpenOrderIndex);

                try (
                        CheckedInputStream is = new CheckedInputStream(
                                EhcacheIOStreams.getInputStream(
                                        ehcacheInputStreamParams.cache,
                                        ehcacheInputStreamParams.cacheKey,
                                        ehcacheInputStreamParams.allowNullStream,
                                        ehcacheInputStreamParams.bufferSize,
                                        ehcacheInputStreamParams.openTimeout),
                                new CRC32()
                        );
                        CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()), new CRC32());
                ) {
                    byte[] buffer = new byte[copyBufferSize];
                    int n;
                    boolean isSyncReleased = false;
                    while ((n = is.read(buffer)) > -1) {
                        //this is to make sure the stream reader is fully opened before telling the other threads to join the party
                        if(!isSyncReleased) {
                            logger.debug("Started the reading from ehcache...Releasing the other waiting threads");
                            ehcacheInputStreamParams.beforeOpenOrderedSync.releaseAll();
                            isSyncReleased = true;
                        }

                        os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write

                        logger.debug("During read - Sleeping for {} millis", ehcacheInputStreamParams.sleepDuringCopyMillis);
                        TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepDuringCopyMillis);
                    }

                    logger.debug("Done with reading all the bytes from ehcache");

                    logger.debug("After Copy Before Close - Sleeping for {} millis", ehcacheInputStreamParams.sleepAfterCopyBeforeCloseMillis);
                    TimeUnit.MILLISECONDS.sleep(ehcacheInputStreamParams.sleepAfterCopyBeforeCloseMillis);

                    returnChecksum = is.getChecksum().getValue();
                }

                logger.debug("Completely finished reading from ehcache (stream closed)");
                logger.debug("Finished callable operation");

                return returnChecksum;
            }
        });

        callableResults.add(new AtomicReference<Long>());
        exceptions.add(new AtomicReference<Throwable>());
    }

    private void addWriteCallable(final EhcacheOutputStreamTestParams ehcacheOuputStreamParams) throws EhcacheStreamException {
        callables.add(new ConcurrentTestCallable() {
            @Override
            public Long call() throws Exception {
                long returnChecksum = -1L;

                Thread.currentThread().setName("Ehcache-Writer-Thread-" + Thread.currentThread().getId());

                logger.debug("Before Open");
                ehcacheOuputStreamParams.beforeOpenOrderedSync.barrier(ehcacheOuputStreamParams.beforeOpenOrderIndex);

                try (
                        CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH), fileReadBufferSize), new CRC32());
                        CheckedOutputStream os = new CheckedOutputStream(
                                EhcacheIOStreams.getOutputStream(
                                        ehcacheOuputStreamParams.cache,
                                        ehcacheOuputStreamParams.cacheKey,
                                        ehcacheOuputStreamParams.override,
                                        ehcacheOuputStreamParams.bufferSize,
                                        ehcacheOuputStreamParams.openTimeout),
                                new CRC32()
                        );
                ) {
                    byte[] buffer = new byte[copyBufferSize];
                    int n;
                    boolean isSyncReleased = false;
                    while ((n = is.read(buffer)) > -1) {
                        os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write

                        //this is to make sure the stream writer is fully opened before telling the other threads to join the party
                        if(!isSyncReleased) {
                            logger.debug("Started the writing from ehcache...Releasing the other waiting threads");
                            ehcacheOuputStreamParams.beforeOpenOrderedSync.releaseAll();
                            isSyncReleased = true;
                        }

                        logger.debug("During write - Sleeping for {} millis", ehcacheOuputStreamParams.sleepDuringCopyMillis);
                        TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepDuringCopyMillis);
                    }

                    logger.debug("Done with writing all the bytes to ehcache");

                    logger.debug("After Copy Before Close - Sleeping for {} millis", ehcacheOuputStreamParams.sleepAfterCopyBeforeCloseMillis);
                    TimeUnit.MILLISECONDS.sleep(ehcacheOuputStreamParams.sleepAfterCopyBeforeCloseMillis);

                    returnChecksum = os.getChecksum().getValue();
                }

                logger.debug("Completely finished writing to ehcache (stream closed)");
                logger.debug("Finished callable operation");

                return returnChecksum;
            }
        });

        callableResults.add(new AtomicReference<Long>());
        exceptions.add(new AtomicReference<Throwable>());
    }

    @Test
    public void testReadDuringWriteEnoughTime() throws EhcacheStreamException, InterruptedException {
        logger.info("============ testReadDuringWriteEnoughTime ====================");

        int WRITER_INDEX = 0;
        int READER_INDEX = 1;

        OrderedSynchronizer orderedSynchronizer = new OrderedSynchronizer();

        //write 1st
        final long ehcacheWriteOpenTimeout = 10000L;
        final int writerBeforeOpenOrderPosition = 0; //first position
        final long writerSleepDuringCopyMillis = 50L;
        final long writerSleepAfterCopyBeforeCloseMillis = 500L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, orderedSynchronizer, writerBeforeOpenOrderPosition, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis
                )
        );

        //read 2nd
        final long ehcacheReadOpenTimeout = 30000L;
        final int readerBeforeOpenOrderPosition = 1; //2nd position
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 0L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, orderedSynchronizer, readerBeforeOpenOrderPosition, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis
                )
        );

        runInThreads();

        Assert.assertNull(exceptions.get(WRITER_INDEX).get()); // write thread should have 0 exception
        Assert.assertNull(exceptions.get(READER_INDEX).get()); // read thread should have 0 exception

        Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(WRITER_INDEX).get().longValue());
        Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(READER_INDEX).get().longValue());

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertTrue(testObjectCheck.getLastWrittenTime() > 0);

        if(PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS) //with explicit locks, the master entry cannot be updated while read locked (hence read time cannot be updated)
            Assert.assertEquals(0, testObjectCheck.getLastReadTime());
        else
            Assert.assertTrue(testObjectCheck.getLastReadTime() > 0);
    }

    @Test
    public void testWriteDuringReadEnoughWaitTime() throws IOException, InterruptedException {
        logger.info("============ testWriteDuringReadEnoughWaitTime ====================");

        int WRITER_INDEX = 0;
        int READER_INDEX = 1;

        OrderedSynchronizer orderedSynchronizer = new OrderedSynchronizer();

        //first, let's add something to cache so the read can start reading something
        StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey());
        Assert.assertEquals(fileFromDisk.getFromChecksum(), copyFileToCacheDesc.getToChecksum());

        final long ehcacheWriteOpenTimeout = 30000L;
        final int writerBeforeOpenOrderPosition = 1; //2nd position
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 0L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, orderedSynchronizer, writerBeforeOpenOrderPosition, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 10000L;
        final int readerBeforeOpenOrderPosition = 0; //first position
        final long readerSleepDuringCopyMillis = 50L; //slow down the read to create a problem in priority write mode
        final long readerSleepAfterCopyBeforeCloseMillis = 500L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, orderedSynchronizer, readerBeforeOpenOrderPosition, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis
                )
        );

        runInThreads();

        if(
                PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS ||
                        PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS
                )
        {
            Assert.assertNull(exceptions.get(WRITER_INDEX).get()); // write thread should have 0 exception
            Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(WRITER_INDEX).get().longValue());

            Assert.assertNull(exceptions.get(READER_INDEX).get()); // read thread should have 0 exception
            Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(READER_INDEX).get().longValue());
        } else if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY){
            Assert.assertNull(exceptions.get(WRITER_INDEX).get()); // write thread should have 0 exception
            Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(WRITER_INDEX).get().longValue());

            Assert.assertNotNull(exceptions.get(1).get()); // read thread should have EhcacheStreamConcurrentException because the write took over
            Assert.assertEquals(EhcacheStreamIllegalStateException.class, exceptions.get(1).get().getClass()); // read thread should have EhcacheStreamIllegalStateException because the write took over
            Assert.assertNull(callableResults.get(1).get());
        }

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertTrue(testObjectCheck.getLastWrittenTime() > 0);

        if(PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS) //with explicit locks, the master entry cannot be updated while read locked (hence read time cannot be updated)
            Assert.assertEquals(0, testObjectCheck.getLastReadTime());
        else
            Assert.assertTrue(testObjectCheck.getLastReadTime() > 0);
    }

    public void testMultipleConcurrentReads(boolean addCachePayloadBeforeReads) throws IOException, InterruptedException {
        final NoopSynchronizer noopSynchronizer = new NoopSynchronizer();

        int initialCacheSize = getCache().getSize();

        // check the stream master from cache at the end of the initial write
        EhcacheStreamMaster testObjectCheckBefore = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());

        Assert.assertEquals(0, initialCacheSize); // should be 0 now
        Assert.assertNotEquals(StreamCopyResultDescriptor.NULL_CHECKSUM, fileFromDisk.getFromChecksum());
        Assert.assertNull(testObjectCheckBefore);

        if(addCachePayloadBeforeReads) {
            //first, let's add something to cache so the read can start reading something
            StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey());

            initialCacheSize = getCache().getSize();
            Assert.assertTrue(initialCacheSize > 1); // should be at least 2 (master key + chunk key)

            Assert.assertEquals(fileFromDisk.getFromChecksum(), copyFileToCacheDesc.getToChecksum());

            // check the stream master from cache at the end of the initial write
            testObjectCheckBefore = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
            Assert.assertNotNull(testObjectCheckBefore);
            logger.debug("BEFORE - EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheckBefore));
            Assert.assertEquals(initialCacheSize - 1, testObjectCheckBefore.getChunkCount());
            Assert.assertEquals(0, testObjectCheckBefore.getReaders());
            Assert.assertEquals(0, testObjectCheckBefore.getWriters());
            Assert.assertTrue(testObjectCheckBefore.getLastWrittenTime() > 0);
            Assert.assertEquals(0, testObjectCheckBefore.getLastReadTime());
        }

        final int threadCount = 10;
        final boolean allowNullStream = false;

        for(int i = 0; i < threadCount; i++) {
            final long ehcacheReadOpenTimeout = 2000L;
            final int readerBeforeOpenOrderPosition = 0; //first position
            final long readerSleepDuringCopyMillis = 0L;
            final long readerSleepAfterCopyBeforeCloseMillis = 500L;

            addReadCallable(
                    new EhcacheInputStreamTestParams(
                            getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, noopSynchronizer, readerBeforeOpenOrderPosition, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis
                    )
            );
        }

        runInThreads();

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // read thread should have 0 exception
            if(addCachePayloadBeforeReads) {
                Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(i).get().longValue());
            } else {
                Assert.assertEquals(0L, callableResults.get(i).get().longValue());
            }
        }

        //check final cache size
        int finalCacheSize = getCache().getSize();

        // check the final stream master from cache at the end
        EhcacheStreamMaster testObjectCheckAfter = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("AFTER - EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheckAfter));

        if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS ||
                PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY){
            if(addCachePayloadBeforeReads) {
                Assert.assertEquals(initialCacheSize, finalCacheSize);
                Assert.assertNotNull(testObjectCheckAfter);
                Assert.assertNotEquals(testObjectCheckBefore, testObjectCheckAfter); //objects should not be equal due to date updates
                Assert.assertTrue(testObjectCheckBefore.equalsNoReadWriteTimes(testObjectCheckAfter)); //object should be equal apart from the timestamps
                Assert.assertTrue(testObjectCheckAfter.getLastReadTime() > testObjectCheckBefore.getLastReadTime());
                Assert.assertEquals(testObjectCheckBefore.getLastWrittenTime(), testObjectCheckAfter.getLastWrittenTime());
            } else {
                Assert.assertEquals(0, finalCacheSize);
                Assert.assertNull(testObjectCheckAfter);
            }
        } else if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS) {
            if(addCachePayloadBeforeReads) {
                Assert.assertEquals(initialCacheSize, finalCacheSize);
                Assert.assertNotNull(testObjectCheckAfter);
                Assert.assertEquals(testObjectCheckBefore, testObjectCheckAfter); //objects should be completely equal (due to the fact that the read do not update the timestamps when using explicit locks
            } else {
                Assert.assertEquals(0, finalCacheSize);
                Assert.assertNull(testObjectCheckAfter);
            }
        }
    }

    @Test
    public void testMultipleConcurrentReads_addCachePayloadBeforeReads() throws IOException, InterruptedException {
        logger.info("============ testMultipleConcurrentReads_addCachePayloadBeforeReads ====================");

        testMultipleConcurrentReads(true);
    }

    @Test
    public void testMultipleConcurrentReads_noCachePayloadBeforeReads() throws IOException, InterruptedException {
        logger.info("============ testMultipleConcurrentReads_noCachePayloadBeforeReads ====================");

        testMultipleConcurrentReads(false);
    }

    @Test
    public void testMultipleWritesOverwriteEnoughWaitTime() throws IOException, InterruptedException {
        logger.info("============ testMultipleWritesOverwriteEnoughWaitTime ====================");

        final NoopSynchronizer noopSynchronizer = new NoopSynchronizer();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final int threadCount = 10;
        final boolean override = true;

        //first, let's do this serially to get an expected checksum
        for(int i = 0; i < threadCount; i++) {
            copyFileToCache(getCacheKey(), override);
        }
        int expectedCacheSize = getCache().getSize();
        Assert.assertTrue(expectedCacheSize > 1); // should be at least 2 (master key + chunk key)

        // check the stream master from cache at the end of the initial write
        EhcacheStreamMaster testObjectCheckBefore = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("BEFORE - EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheckBefore));

        Assert.assertEquals(expectedCacheSize - 1, testObjectCheckBefore.getChunkCount());
        Assert.assertEquals(0, testObjectCheckBefore.getReaders());
        Assert.assertEquals(0, testObjectCheckBefore.getWriters());

        //cleanup and wait a bit just to be safe
        getCache().removeAll();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        for(int i = 0; i < threadCount; i++) {
            final long ehcacheWriteOpenTimeout = 30000L; //long enough timeout
            final int writerBeforeOpenOrderPosition = 0; //first position
            final long writerSleepDuringCopyMillis = 0L;
            final long writerSleepAfterCopyBeforeCloseMillis = 500L;

            addWriteCallable(
                    new EhcacheOutputStreamTestParams(
                            getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, noopSynchronizer, writerBeforeOpenOrderPosition, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis
                    )
            );
        }

        runInThreads();

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // read thread should have 0 exception
            Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(i).get().longValue());
        }

        //check final cache size
        Assert.assertEquals(expectedCacheSize, getCache().getSize());

        // check the final stream master from cache at the end
        EhcacheStreamMaster testObjectCheckAfter = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("AFTER - EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheckAfter));
        Assert.assertTrue(testObjectCheckBefore.equalsNoReadWriteTimes(testObjectCheckAfter));

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertTrue(testObjectCheck.getLastWrittenTime() > 0);
        Assert.assertEquals(0,testObjectCheck.getLastReadTime());
    }

    @Test
    public void testMultipleWritesAppendEnoughWaitTime() throws IOException, InterruptedException {
        logger.info("============ testMultipleWritesAppendEnoughWaitTime ====================");

        final NoopSynchronizer noopSynchronizer = new NoopSynchronizer();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        final int threadCount = 10;
        final boolean override = false;

        //first, let's do this serially to get an expected checksum
        for(int i = 0; i < threadCount; i++) {
            copyFileToCache(getCacheKey(), override);
        }

        int expectedCacheSize = getCache().getSize();
        Assert.assertTrue(expectedCacheSize > 1); // should be at least 2 (master key + chunk key)

        //cleanup and wait a bit just to be safe
        getCache().removeAll();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //now, run in threads
        for(int i = 0; i < threadCount; i++) {
            final long ehcacheWriteOpenTimeout = 30000L; //long enough timeout
            final int writerBeforeOpenOrderPosition = 0; //first position
            final long writerSleepDuringCopyMillis = 0L;
            final long writerSleepAfterCopyBeforeCloseMillis = 500L;

            addWriteCallable(
                    new EhcacheOutputStreamTestParams(
                            getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, noopSynchronizer, writerBeforeOpenOrderPosition, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis
                    )
            );
        }

        runInThreads();

        for(int i = 0; i < threadCount; i++) {
            Assert.assertNull(exceptions.get(i).get()); // should have 0 exception
            Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(i).get().longValue());
        }
        Assert.assertEquals(expectedCacheSize, getCache().getSize());

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertTrue(testObjectCheck.getLastWrittenTime() > 0);
        Assert.assertEquals(0,testObjectCheck.getLastReadTime());
    }

    @Test
    public void testReadDuringWrite_ReadTimeoutReached() throws IOException, InterruptedException {
        logger.info("============ testReadDuringWrite_ReadTimeoutReached ====================");

        int WRITER_INDEX = 0;
        int READER_INDEX = 1;

        final OrderedSynchronizer orderedSynchronizer = new OrderedSynchronizer();

        final long ehcacheWriteOpenTimeout = 500L;
        final int writerBeforeOpenOrderPosition = 0; //first position: ensured write thread starts before read thread
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 1000L; //this is larger than the read lock
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, orderedSynchronizer, writerBeforeOpenOrderPosition, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 500L;
        final int readerBeforeOpenOrderPosition = 1; //2nd position: ensured read thread starts after the write one
        final long readerSleepDuringCopyMillis = 0L;
        final long readerSleepAfterCopyBeforeCloseMillis = 0L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, orderedSynchronizer, readerBeforeOpenOrderPosition, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis
                )
        );

        runInThreads();

        Assert.assertNull(exceptions.get(WRITER_INDEX).get()); // write thread should have 0 exception
        Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(WRITER_INDEX).get().longValue());

        Assert.assertTrue(null != exceptions.get(READER_INDEX).get() && exceptions.get(READER_INDEX).get() instanceof EhcacheStreamTimeoutException); // read thread should have 1 exception
        Assert.assertNull(callableResults.get(READER_INDEX).get());

        // check the final counter
        EhcacheStreamMaster testObjectCheck = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheck));
        Assert.assertEquals(0,testObjectCheck.getWriters());
        Assert.assertEquals(0,testObjectCheck.getReaders());
        Assert.assertEquals(0,testObjectCheck.getLastReadTime());
        Assert.assertTrue(testObjectCheck.getLastWrittenTime() > 0);
    }

    @Test
    public void testWriteDuringRead_WriteTimeoutReached() throws IOException, InterruptedException {
        logger.info("============ testWriteDuringRead_WriteTimeoutReached ====================");

        int WRITER_INDEX = 0;
        int READER_INDEX = 1;

        final OrderedSynchronizer orderedSynchronizer = new OrderedSynchronizer();

        Assert.assertEquals(0, getCache().getSize()); // should be 0 now

        //first, let's add something to cache so the read can start reading something
        StreamCopyResultDescriptor copyFileToCacheDesc = copyFileToCache(getCacheKey());
        int expectedCacheSize = getCache().getSize();

        Assert.assertTrue(expectedCacheSize > 1); // should be at least 2 (master key + chunk key)
        Assert.assertEquals(fileFromDisk.getFromChecksum(), copyFileToCacheDesc.getToChecksum());

        // check the state of the stream entry
        EhcacheStreamMaster testObjectCheckBefore = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        Assert.assertNotNull(testObjectCheckBefore);
        logger.debug("EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheckBefore));
        Assert.assertEquals(0,testObjectCheckBefore.getWriters());
        Assert.assertEquals(0,testObjectCheckBefore.getReaders());
        Assert.assertTrue(testObjectCheckBefore.getLastWrittenTime() > 0);
        Assert.assertEquals(0, testObjectCheckBefore.getLastReadTime());

        final long ehcacheWriteOpenTimeout = 500L;
        final int writerBeforeOpenOrderPosition = 1; //2nd position: ensured write thread starts after read thread
        final long writerSleepDuringCopyMillis = 0L;
        final long writerSleepAfterCopyBeforeCloseMillis = 0L;
        final boolean override = true;

        addWriteCallable(
                new EhcacheOutputStreamTestParams(
                        getCache(), getCacheKey(), override, ehcacheWriteBufferSize, ehcacheWriteOpenTimeout, orderedSynchronizer, writerBeforeOpenOrderPosition, writerSleepDuringCopyMillis, writerSleepAfterCopyBeforeCloseMillis
                )
        );

        final long ehcacheReadOpenTimeout = 500L;
        final int readerBeforeOpenOrderPosition = 0; //first position: ensured read thread starts before write thread
        final long readerSleepDuringCopyMillis = 50L;
        final long readerSleepAfterCopyBeforeCloseMillis = 1000L;
        final boolean allowNullStream = false;

        addReadCallable(
                new EhcacheInputStreamTestParams(
                        getCache(), getCacheKey(), allowNullStream, ehcacheReadBufferSize, ehcacheReadOpenTimeout, orderedSynchronizer, readerBeforeOpenOrderPosition, readerSleepDuringCopyMillis, readerSleepAfterCopyBeforeCloseMillis
                )
        );

        runInThreads();

        if(
                PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS ||
                        PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS
                )
        {
            Assert.assertNotNull(exceptions.get(WRITER_INDEX).get()); // write thread should have 0 exception
            Assert.assertEquals(EhcacheStreamTimeoutException.class, exceptions.get(WRITER_INDEX).get().getClass()); // write thread should have 1 exception
            Assert.assertNull(callableResults.get(WRITER_INDEX).get());

            Assert.assertNull(exceptions.get(READER_INDEX).get()); // read thread should have 0 exception
            Assert.assertEquals(fileFromDisk.getFromChecksum(), callableResults.get(READER_INDEX).get().longValue());
        } else if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY){
            Assert.assertNull(exceptions.get(WRITER_INDEX).get()); // write thread should have 0 exception
            Assert.assertEquals(copyFileToCacheDesc.getToChecksum(), callableResults.get(WRITER_INDEX).get().longValue());

            Assert.assertNotNull(exceptions.get(READER_INDEX).get()); // read thread should have EhcacheStreamConcurrentException because the write took over
            Assert.assertEquals(EhcacheStreamIllegalStateException.class, exceptions.get(READER_INDEX).get().getClass());
            Assert.assertNull(callableResults.get(READER_INDEX).get());
        }

        Assert.assertEquals(expectedCacheSize, getCache().getSize()); // should be 0 now

        // check the final counter
        EhcacheStreamMaster testObjectCheckAfter = streamUtilsInternal.getStreamMasterFromCache(getCacheKey());
        logger.debug("Final EhcacheStreamMaster check from cache: {}", EhcacheStreamUtilsInternal.toStringSafe(testObjectCheckAfter));
        Assert.assertNotNull(testObjectCheckAfter);
        Assert.assertEquals(0,testObjectCheckAfter.getWriters());
        Assert.assertEquals(0, testObjectCheckAfter.getReaders());

        if(PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS) //with explicit locks, the master entry cannot be updated while read locked (hence read time cannot be updated)
            Assert.assertEquals(0, testObjectCheckAfter.getLastReadTime());
        else
            Assert.assertTrue(testObjectCheckAfter.getLastReadTime() > 0);

        if(PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY) //with write priority mode, the write will happen
            Assert.assertTrue(testObjectCheckBefore.getLastWrittenTime() < testObjectCheckAfter.getLastWrittenTime()); // write happened, so the last write time should be higher than before
        else
            Assert.assertEquals(testObjectCheckBefore.getLastWrittenTime(), testObjectCheckAfter.getLastWrittenTime()); // write did not happen, so the last write time should be the same as before
    }

    public interface Synchronizer {
        void barrier(int index);
        void releaseAll();
        void release();
    }

    //if the threads pass a positive index, it will wait until the internal index equals the thread index.
    //if the threads pass -1, special case that'll make the thread go anyway
    public class NoopSynchronizer implements Synchronizer {
        @Override
        public void barrier(int index) {
            ;;
        }

        @Override
        public void releaseAll() {
            ;;
        }

        @Override
        public void release() {
            ;;
        }
    }

    //if the threads pass a positive index, it will wait until the internal index equals the thread index.
    //if the threads pass -1, special case that'll make the thread go anyway
    public class OrderedSynchronizer implements Synchronizer {
        private int index = 0;
        private static final int SPECIAL = -1;

        public synchronized void barrier(int index){
            while(this.index != SPECIAL && this.index != index){
                try {
                    logger.debug("My index {} is not equal to current index {}. Going to WAIT state...", index, this.index);
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Thread interrupted.", e);
                }
            }
        }

        public synchronized void releaseAll() {
            release(true);
        }

        public synchronized void release() {
            release(false);
        }

        public synchronized void release(boolean all){
            //update the index
            if(all)
                this.index = SPECIAL;
            else
                this.index++;

            logger.debug("Current index updated to {}. Waking up all other WAITING threads.", index);
            notifyAll();
        }
    }
}
