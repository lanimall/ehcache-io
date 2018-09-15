package org.ehcache.extensions.io.impl;

import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsFactory;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class EhcacheStreamUtilsTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamUtilsTest.class);

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        System.out.println("============ Starting EhcacheStreamUtilsTest ====================");
        sysPropDefaultSetup();
        cacheStart();
        generateBigInputFile();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cacheShutdown();
        cleanBigInputFile();
        sysPropDefaultCleanup();
        System.out.println("============ Finished EhcacheStreamUtilsTest ====================");
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

        Assert.assertFalse(removed);
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
}