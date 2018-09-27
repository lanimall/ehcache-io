package org.ehcache.extensions.io.impl;

import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsFactory;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
        cleanupParameterizedProperties();
    }

    @Test
    public void testRemoveExistingStreamEntry() throws Exception {
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
        long openTimeout = 10000L;
        Assert.assertEquals(0, getCache().getSize());

        boolean removed = EhcacheStreamUtilsFactory.getUtils(getCache()).removeStreamEntry(getCacheKey(), openTimeout);

        Assert.assertTrue(removed);
        Assert.assertEquals(0, getCache().getSize());
    }

    @Test
    public void testContainsExistingStreamEntry() throws Exception {
        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        boolean found = EhcacheStreamUtilsFactory.getUtils(getCache()).containsStreamEntry(getCacheKey());

        Assert.assertTrue(found);
    }

    @Test
    public void testContainsNonExistingStreamEntry() throws Exception {
        Assert.assertEquals(0, getCache().getSize());

        boolean found = EhcacheStreamUtilsFactory.getUtils(getCache()).containsStreamEntry(getCacheKey());

        Assert.assertFalse(found);
    }

    @Test
    public void testGetAllStreamEntryKeys() throws Exception {
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
        boolean expirationCheck = true;

        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        List keys = EhcacheStreamUtilsFactory.getUtils(getCache()).getAllStreamEntryKeys(expirationCheck);

        Assert.assertNotNull(keys);
        Assert.assertEquals(1, keys.size());
        Assert.assertEquals(getCacheKey(), keys.get(0));
    }

    @Test
    public void testGetAllStreamEntryKeysFilteredByStateWrites() throws Exception {
        final boolean cacheWriteOverride = true;
        final int cacheWriteBufferSize = PropertyUtils.getOutputStreamBufferSize();
        boolean expirationCheck = true;
        boolean includeCurrentWrites = true;
        boolean includeCurrentReads = false;

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

            List keys = EhcacheStreamUtilsFactory.getUtils(getCache()).getAllStreamEntryKeysFilteredByState(expirationCheck, includeCurrentWrites, includeCurrentReads);

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
    }

    @Test
    public void testGetAllStreamEntryKeysFilteredByStateReads() throws Exception {
        final int cacheReadBufferSize = PropertyUtils.getInputStreamBufferSize();
        boolean expirationCheck = true;
        boolean includeCurrentWrites = false;
        boolean includeCurrentReads = true;

        int iterations = 10;
        List<InputStream> inputStreams = new ArrayList<InputStream>(iterations);
        try {
            for(int i = 0; i < iterations; i++) {
                String cacheKey = getCacheKey().toString() + i;

                //will copy a good value in cache
                copyFileToCache(cacheKey);

                //and now, will read to cache another value, BUT will not close the stream
                InputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(getCache(), cacheKey, false, cacheReadBufferSize), new CRC32());
                readFileFromCacheStream(is);

                inputStreams.add(is);
            }

            List keys = EhcacheStreamUtilsFactory.getUtils(getCache()).getAllStreamEntryKeysFilteredByState(expirationCheck, includeCurrentWrites, includeCurrentReads);

            Assert.assertNotNull(keys);
            if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.WRITE_PRIORITY){
                Assert.assertEquals(0, keys.size());
            } else if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_WITHLOCKS) {
                Assert.assertEquals(0, keys.size());
            } else if (PropertyUtils.getEhcacheIOStreamsConcurrencyMode() == PropertyUtils.ConcurrencyMode.READ_COMMITTED_CASLOCKS) {
                Assert.assertEquals(iterations, keys.size());
                for (int i = 0; i < iterations; i++) {
                    Assert.assertEquals(getCacheKey().toString() + i, keys.get(keys.indexOf(getCacheKey().toString() + i)));
                }
            }
        } finally {
            for(InputStream is: inputStreams) {
                if (null != is)
                    is.close();
            }
        }
    }
}