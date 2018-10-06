package org.ehcache.extensions.io.impl.utils;

import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

@RunWith(Parameterized.class)
public class EhcacheStreamUtilsTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamUtilsTest.class);

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        logger.debug("============ Starting EhcacheStreamUtilsTest ====================");
        sysPropDefaultSetup();
        cacheStart();
        generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        sysPropDefaultCleanup();
        logger.debug("============ Finished EhcacheStreamUtilsTest ====================");
    }

    @Before
    public void setup() throws Exception {
        setupParameterizedProperties();
        cacheSetUp();
        printAllTestProperties();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        cleanupParameterizedProperties();
    }

    @Test
    public void testRemoveExistingStreamEntry() throws Exception {
        logger.info("============ testRemoveExistingStreamEntry ====================");

        long openTimeout = 10000L;

        Assert.assertEquals(0, getCache().getSize());

        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        boolean removed = EhcacheStreamUtilsFactory.getUtils(getCache()).removeStreamEntry(getCacheKey(), openTimeout);

        Assert.assertTrue(removed);
        Assert.assertEquals(0, getCache().getSize());
    }

    @Test
    public void testRemoveNonExistingStreamEntry() throws Exception {
        logger.info("============ testRemoveNonExistingStreamEntry ====================");

        long openTimeout = 10000L;
        Assert.assertEquals(0, getCache().getSize());

        boolean removed = EhcacheStreamUtilsFactory.getUtils(getCache()).removeStreamEntry(getCacheKey(), openTimeout);

        Assert.assertTrue(removed);
        Assert.assertEquals(0, getCache().getSize());
    }

    @Test
    public void testContainsExistingStreamEntry() throws Exception {
        logger.info("============ testContainsExistingStreamEntry ====================");

        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        boolean found = EhcacheStreamUtilsFactory.getUtils(getCache()).containsStreamEntry(getCacheKey());

        Assert.assertTrue(found);
    }

    @Test
    public void testContainsNonExistingStreamEntry() throws Exception {
        logger.info("============ testContainsNonExistingStreamEntry ====================");

        Assert.assertEquals(0, getCache().getSize());

        boolean found = EhcacheStreamUtilsFactory.getUtils(getCache()).containsStreamEntry(getCacheKey());

        Assert.assertFalse(found);
    }

    @Test
    public void testGetAllStreamEntryKeys() throws Exception {
        logger.info("============ testGetAllStreamEntryKeys ====================");

        boolean expirationCheck = false;

        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        List keys = EhcacheStreamUtilsFactory.getUtils(getCache()).getAllStreamEntryKeys(expirationCheck);

        Assert.assertNotNull(keys);
        Assert.assertEquals(1, keys.size());
        Assert.assertEquals(getCacheKey(), keys.get(0));
    }

    @Test
    public void testGetAllStreamEntryKeysExpirationCheck() throws Exception {
        logger.info("============ testGetAllStreamEntryKeysExpirationCheck ====================");

        boolean expirationCheck = true;

        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        List keys = EhcacheStreamUtilsFactory.getUtils(getCache()).getAllStreamEntryKeys(expirationCheck);

        Assert.assertNotNull(keys);
        Assert.assertEquals(1, keys.size());
        Assert.assertEquals(getCacheKey(), keys.get(0));
    }

    @Test
    public void testGetAllStreamEntryKeysFilteredWritesOnly() throws Exception {
        logger.info("============ testGetAllStreamEntryKeysFilteredWritesOnly ====================");

        final boolean cacheWriteOverride = true;
        final int cacheWriteBufferSize = PropertyUtils.getOutputStreamBufferSize();
        boolean expirationCheck = true;
        boolean includeCurrentWrites = true;

        //will copy a good value in cache
        copyFileToCache(getCacheKey());

        int iterations = 10;
        List<OutputStream> outputStreams = new ArrayList<OutputStream>(iterations);
        try {
            for (int i = 0; i < iterations; i++) {
                String cacheKey = getCacheKey().toString() + i;
                OutputStream os = new CheckedOutputStream(EhcacheIOStreams.getOutputStream(getCache(), cacheKey, cacheWriteOverride, cacheWriteBufferSize), new CRC32());
                copyFileToCacheStream(os);

                outputStreams.add(os);
            }

            List keys = EhcacheStreamUtilsFactory.getUtils(getCache()).getAllStreamEntryKeys(expirationCheck, false, false, false, includeCurrentWrites);

            Assert.assertNotNull(keys);
            Assert.assertEquals(iterations, keys.size());
            for (int i = 0; i < iterations; i++) {
                Assert.assertEquals(getCacheKey().toString() + i, keys.get(keys.indexOf(getCacheKey().toString() + i)));
            }
        } finally {
            for(OutputStream os: outputStreams) {
                if (null != os)
                    os.close();
            }
        }
        //and now, will copy to cache another value, BUT will not close the stream
        OutputStream os = null;
        try {

        } finally {
            if(null != os)
                os.close();
        }

        logger.info("============ testGetAllStreamEntryKeysFilteredWritesOnly end ====================");
    }

    @Test
    public void testGetAllStreamEntryKeysFilteredReadsOnly() throws Exception {
        logger.info("============ testGetAllStreamEntryKeysFilteredReadsOnly ====================");

        final int cacheReadBufferSize = PropertyUtils.getInputStreamBufferSize();
        boolean expirationCheck = true;
        boolean includeCurrentReads = true;
        int iterations = 10;

        if(PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS ||
                PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY)
        {
            List<InputStream> inputStreams = null;
            try {
                //leave entries in open state on purpose to see if we catch the keys totals properly.
                //NOTE: this approach is only safe with CAS because with explicit locks, I notice deadlocks leaving locks opened with LOCAL_HEAP tests
                //This is because ehcache explicit locking with local heap caches works by locking segments, not single keys...
                //and as such, a left opened read lock will block a full segment against writing...which will deadlock the test
                inputStreams = new ArrayList<InputStream>(iterations);
                for (int i = 0; i < iterations; i++) {
                    String cacheKey = getCacheKey().toString() + i * 1123;

                    //will copy a good value in cache
                    copyFileToCache(cacheKey);

                    //and now, will read to cache another value, BUT will not close the stream
                    InputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), cacheKey, false, cacheReadBufferSize), new CRC32());
                    readFileFromCacheStream(is);

                    // >>> HERE we leave open for now!

                    inputStreams.add(is);
                }

                List keys = EhcacheStreamUtilsFactory.getUtils(getCache()).getAllStreamEntryKeys(expirationCheck, false, false, includeCurrentReads, false);

                Assert.assertNotNull(keys);
                if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY) {
                    Assert.assertEquals(0, keys.size());
                } else if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS) {
                    Assert.assertEquals(iterations, keys.size());
                    for (int i = 0; i < iterations; i++) {
                        Assert.assertEquals(getCacheKey().toString() + i * 1123, keys.get(keys.indexOf(getCacheKey().toString() + i * 1123)));
                    }
                }
            } finally {
                for(InputStream is: inputStreams) {
                    if (null != is)
                        is.close();
                }
            }
        } else {
            List<InputStream> inputStreams = null;
            try {
                inputStreams = new ArrayList<InputStream>(iterations);
                for (int i = 0; i < iterations; i++) {
                    String cacheKey = getCacheKey().toString() + i * 1123;

                    //will copy a good value in cache
                    copyFileToCache(cacheKey);

                    //and now, will read to cache another value, BUT will not close the stream
                    try (
                            InputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), cacheKey, false, cacheReadBufferSize), new CRC32());
                    )
                    {
                        readFileFromCacheStream(is);

                        // >>> HERE we will close just to avoid deadlocks (and anyway, with explicit locking, there's not a godo way to query the locks in play...
                        // so this getAllStreamEntryKeysFiltered really does not make sense with explicit locks...so not a big deal here.

                    }
                }

                List keys = EhcacheStreamUtilsFactory.getUtils(getCache()).getAllStreamEntryKeys(expirationCheck, false, false, includeCurrentReads, false);
                Assert.assertEquals(0, keys.size());
            } finally {
                for(InputStream is: inputStreams) {
                    if (null != is)
                        is.close();
                }
            }
        }

        logger.info("============ testGetAllStreamEntryKeysFilteredReadsOnly end ====================");
    }
}