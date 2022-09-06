package org.ehcache.extensions.io.impl.model;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Created by fabien.sanglier on 10/23/18.
 */
public class EhcacheStreamMasterTest {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamMasterTest.class);

    private EhcacheStreamMaster createRandomEhcacheStreamMaster(int numberChunks, Random rnd){
        EhcacheStreamMaster ehcacheStreamMaster1 = new EhcacheStreamMaster();

        for(int i = 0 ; i < numberChunks ; i++){
            int index = rnd.nextInt();
            int size = rnd.nextInt();
            long checksum = rnd.nextLong();

            ehcacheStreamMaster1.addChunk(index, size, checksum);
        }

        long writers = rnd.nextInt(20);
        long readers = rnd.nextInt(20);
        for(int j = 0 ; j < writers ; j++) {
            ehcacheStreamMaster1.addWriter();
        }

        for(int k = 0 ; k < readers ; k++) {
            ehcacheStreamMaster1.addReader();
        }

        return ehcacheStreamMaster1;
    }

    @Test
    public void testAddChunkCounts() throws Exception {
        int numberChunks = 100;
        int[] chunkIndices = new int[numberChunks];
        long[] chunkSizes = new long[numberChunks];
        long[] chunkChecksums = new long[numberChunks];

        EhcacheStreamMaster ehcacheStreamMaster = new EhcacheStreamMaster();
        Random rnd = new Random(System.currentTimeMillis());
        for(int i = 0 ; i < numberChunks ; i++){
            int index = rnd.nextInt();
            long size = rnd.nextLong();
            long checksum = rnd.nextLong();

            ehcacheStreamMaster.addChunk(index, size, checksum);

            //save these numbers for later
            chunkIndices[i] = index;
            chunkSizes[i] = size;
            chunkChecksums[i] = checksum;
        }

        //check the sizes and checksums
        int[] chunkIndexArray = ehcacheStreamMaster.getAllChunkIndices();
        long[] chunkSizeArray = ehcacheStreamMaster.getAllChunkSizeInBytes();
        long[] chunkChecksumArray = ehcacheStreamMaster.getAllChunkChecksums();

        Assert.assertEquals(numberChunks, ehcacheStreamMaster.getChunkCount());
        Assert.assertEquals(numberChunks, chunkSizeArray.length);
        Assert.assertEquals(numberChunks, chunkChecksumArray.length);

        Assert.assertArrayEquals(chunkIndices, chunkIndexArray);
        Assert.assertArrayEquals(chunkSizes, chunkSizeArray);
        Assert.assertArrayEquals(chunkChecksums, chunkChecksumArray);
    }

    @Test
    public void testResetChunkCounts() throws Exception {
        int numberChunks = 100;
        int[] chunkIndices = new int[numberChunks];
        long[] chunkSizes = new long[numberChunks];
        long[] chunkChecksums = new long[numberChunks];

        EhcacheStreamMaster ehcacheStreamMaster = new EhcacheStreamMaster();
        Random rnd = new Random(System.currentTimeMillis());
        for(int i = 0 ; i < numberChunks ; i++){
            int index = rnd.nextInt();
            long size = rnd.nextLong();
            long checksum = rnd.nextLong();

            ehcacheStreamMaster.addChunk(index, size, checksum);

            //save these numbers for later
            chunkIndices[i] = index;
            chunkSizes[i] = size;
            chunkChecksums[i] = checksum;
        }

        Assert.assertEquals(numberChunks, ehcacheStreamMaster.getChunkCount());

        ehcacheStreamMaster.resetChunkCount();

        Assert.assertEquals(0, ehcacheStreamMaster.getChunkCount());
    }

    @Test
    public void testClone() throws Exception {
        int numberChunks = 100;

        EhcacheStreamMaster ehcacheStreamMaster1 = new EhcacheStreamMaster();
        EhcacheStreamMaster ehcacheStreamMaster2 = new EhcacheStreamMaster();

        Random rnd = new Random(System.currentTimeMillis());

        for(int i = 0 ; i < numberChunks ; i++){
            int index = rnd.nextInt();
            int size = rnd.nextInt();
            long checksum = rnd.nextLong();

            ehcacheStreamMaster1.addChunk(index, size, checksum);
            ehcacheStreamMaster2.addChunk(index, size, checksum);
        }

        long writers = rnd.nextInt(20);
        long readers = rnd.nextInt(20);
        for(int j = 0 ; j < writers ; j++) {
            ehcacheStreamMaster1.addWriter();
            ehcacheStreamMaster2.addWriter();
        }

        for(int k = 0 ; k < readers ; k++) {
            ehcacheStreamMaster1.addReader();
            ehcacheStreamMaster2.addReader();
        }

        Assert.assertEquals(ehcacheStreamMaster1, ehcacheStreamMaster2);
        Assert.assertEquals(ehcacheStreamMaster1, ehcacheStreamMaster1.clone());
        Assert.assertEquals(ehcacheStreamMaster2, ehcacheStreamMaster1.clone());
    }

    @Test
    public void testDeepCopy() throws Exception {
        int numberChunks = 100;

        EhcacheStreamMaster ehcacheStreamMaster1 = new EhcacheStreamMaster();
        EhcacheStreamMaster ehcacheStreamMaster2 = new EhcacheStreamMaster();

        Random rnd = new Random(System.currentTimeMillis());

        for(int i = 0 ; i < numberChunks ; i++){
            int index = rnd.nextInt();
            int size = rnd.nextInt();
            long checksum = rnd.nextLong();

            ehcacheStreamMaster1.addChunk(index, size, checksum);
            ehcacheStreamMaster2.addChunk(index, size, checksum);
        }

        long writers = rnd.nextInt(20);
        long readers = rnd.nextInt(20);
        for(int j = 0 ; j < writers ; j++) {
            ehcacheStreamMaster1.addWriter();
            ehcacheStreamMaster2.addWriter();
        }

        for(int k = 0 ; k < readers ; k++) {
            ehcacheStreamMaster1.addReader();
            ehcacheStreamMaster2.addReader();
        }

        Assert.assertEquals(ehcacheStreamMaster1, ehcacheStreamMaster2);
        Assert.assertEquals(ehcacheStreamMaster1, EhcacheStreamMaster.deepCopy(ehcacheStreamMaster1));
        Assert.assertEquals(ehcacheStreamMaster2, EhcacheStreamMaster.deepCopy(ehcacheStreamMaster1));
    }

    @Test
    public void testEquals() throws Exception {
        int numberChunks = 100;

        EhcacheStreamMaster ehcacheStreamMaster1 = new EhcacheStreamMaster();
        EhcacheStreamMaster ehcacheStreamMaster2 = new EhcacheStreamMaster();

        Random rnd = new Random(System.currentTimeMillis());

        for(int i = 0 ; i < numberChunks ; i++){
            int index = rnd.nextInt();
            int size = rnd.nextInt();
            long checksum = rnd.nextLong();

            ehcacheStreamMaster1.addChunk(index, size, checksum);
            ehcacheStreamMaster2.addChunk(index, size, checksum);
        }

        long writers = rnd.nextInt(20);
        long readers = rnd.nextInt(20);
        for(int j = 0 ; j < writers ; j++) {
            ehcacheStreamMaster1.addWriter();
            ehcacheStreamMaster2.addWriter();
        }

        for(int k = 0 ; k < readers ; k++) {
            ehcacheStreamMaster1.addReader();
            ehcacheStreamMaster2.addReader();
        }

        Assert.assertEquals(ehcacheStreamMaster1, ehcacheStreamMaster2);
        Assert.assertTrue(ehcacheStreamMaster1.equalsNoReadWriteTimes(ehcacheStreamMaster2));
        Assert.assertTrue(EhcacheStreamMaster.compare(ehcacheStreamMaster1, ehcacheStreamMaster2));
    }

    @Test
    public void testNotEqual() throws Exception {
        int numberChunks = 100;

        Random rnd1 = new Random(System.nanoTime());
        EhcacheStreamMaster ehcacheStreamMaster1 = createRandomEhcacheStreamMaster(numberChunks, rnd1);

        Thread.sleep(10);

        Random rnd2 = new Random(System.nanoTime());
        EhcacheStreamMaster ehcacheStreamMaster2 = createRandomEhcacheStreamMaster(numberChunks, rnd2);

        Assert.assertNotEquals(ehcacheStreamMaster1, ehcacheStreamMaster2);
    }

    @Test
    public void testHashcodeEqual() throws Exception {
        int numberChunks = 100;

        EhcacheStreamMaster ehcacheStreamMaster1 = new EhcacheStreamMaster();
        EhcacheStreamMaster ehcacheStreamMaster2 = new EhcacheStreamMaster();

        Random rnd = new Random(System.currentTimeMillis());

        for(int i = 0 ; i < numberChunks ; i++){
            int index = rnd.nextInt();
            int size = rnd.nextInt();
            long checksum = rnd.nextLong();

            ehcacheStreamMaster1.addChunk(index, size, checksum);
            ehcacheStreamMaster2.addChunk(index, size, checksum);
        }

        long writers = rnd.nextInt(20);
        long readers = rnd.nextInt(20);
        for(int j = 0 ; j < writers ; j++) {
            ehcacheStreamMaster1.addWriter();
            ehcacheStreamMaster2.addWriter();
        }

        for(int k = 0 ; k < readers ; k++) {
            ehcacheStreamMaster1.addReader();
            ehcacheStreamMaster2.addReader();
        }

        Assert.assertEquals(ehcacheStreamMaster1.hashCode(), ehcacheStreamMaster2.hashCode());
    }

    @Test
    public void testHashcodeNotEqual() throws Exception {
        int numberChunks = 100;

        Random rnd1 = new Random(System.nanoTime());
        EhcacheStreamMaster ehcacheStreamMaster1 = createRandomEhcacheStreamMaster(numberChunks,rnd1);

        Thread.sleep(10);

        Random rnd2 = new Random(System.nanoTime());
        EhcacheStreamMaster ehcacheStreamMaster2 = createRandomEhcacheStreamMaster(numberChunks,rnd2);

        Assert.assertNotEquals(ehcacheStreamMaster1.hashCode(), ehcacheStreamMaster2.hashCode());
    }
}
