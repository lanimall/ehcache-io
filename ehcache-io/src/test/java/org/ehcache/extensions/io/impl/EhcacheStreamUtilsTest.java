package org.ehcache.extensions.io.impl;

import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.*;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

public class EhcacheStreamUtilsTest extends EhcacheStreamingTestsBase {

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
        cacheSetUp();
    }

    @After
    public void cleanup() throws IOException {
        cacheCleanUp();
    }

    @Test
    public void testRemoveExistingStreamEntry() throws Exception {
        long openTimeout = 10000L;

        Assert.assertTrue(getCache().getSize() == 0);

        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        boolean removed = new EhcacheStreamUtils(getCache()).removeStreamEntry(getCacheKey(), openTimeout);

        Assert.assertTrue(removed);
        Assert.assertTrue(getCache().getSize() == 0);
    }

    @Test
    public void testRemoveNonExistingStreamEntry() throws Exception {
        long openTimeout = 10000L;
        Assert.assertTrue(getCache().getSize() == 0);

        boolean removed = new EhcacheStreamUtils(getCache()).removeStreamEntry(getCacheKey(), openTimeout);

        Assert.assertFalse(removed);
        Assert.assertTrue(getCache().getSize() == 0);
    }

    @Test
    public void testContainsExistingStreamEntry() throws Exception {
        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        boolean found = new EhcacheStreamUtils(getCache()).containsStreamEntry(getCacheKey());

        Assert.assertTrue(found);
    }

    @Test
    public void testContainsNonExistingStreamEntry() throws Exception {
        Assert.assertTrue(getCache().getSize() == 0);

        boolean found = new EhcacheStreamUtils(getCache()).containsStreamEntry(getCacheKey());

        Assert.assertFalse(found);
    }

    @Test
    public void testGetAllStreamEntryKeys() throws Exception {
        boolean expirationCheck = false;

        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        List keys = new EhcacheStreamUtils(getCache()).getAllStreamEntryKeys(expirationCheck);

        Assert.assertTrue(null != keys);
        Assert.assertTrue(keys.size() == 1);
        Assert.assertTrue(keys.get(0).equals(getCacheKey()));
    }

    @Test
    public void testGetAllStreamEntryKeysExpirationCheck() throws Exception {
        boolean expirationCheck = true;

        copyFileToCache(getCacheKey());

        Assert.assertTrue(getCache().getSize() > 1); // should be at least 2 (master key + chunk key)

        List keys = new EhcacheStreamUtils(getCache()).getAllStreamEntryKeys(expirationCheck);

        Assert.assertTrue(null != keys);
        Assert.assertTrue(keys.size() == 1);
        Assert.assertTrue(keys.get(0).equals(getCacheKey()));
    }
}