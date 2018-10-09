package org.ehcache.extensions.io.impl.utils;

import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 10/8/18.
 */
public class WaitTest extends EhcacheStreamingTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamCasTest.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    @Test
    public void testConstantWaitStrategy() throws Exception {
        long waitTime = 200;
        ConstantWaitStrategy waitStrategy = new ConstantWaitStrategy(waitTime);
        for(int i = 0; i < 500; i++){
            long actualWait = waitStrategy.getWait(i);
            Assert.assertEquals(waitTime, actualWait);

            if(isDebug)
                logger.debug("actualWait = {}", actualWait);
        }
    }

    @Test
    public void testExponentialWaitStrategyNoJitter() throws Exception {
        final long base = 1;
        final long cap = 200;
        final boolean jitter = false;

        ExponentialWait waitStrategy = new ExponentialWait(base, cap, jitter);
        for(int i = 0; i < 500; i++){
            long actualWait = waitStrategy.getWait(i);

            Assert.assertEquals(ExponentialWait.getWaitTimeNoJitter(cap, base, i), actualWait);
            Assert.assertTrue(actualWait > 0);
            Assert.assertTrue(actualWait <= cap);

            if(isDebug)
                logger.debug("actualWait = {}", actualWait);
        }
    }

    @Test
    public void testExponentialWaitStrategyWithJitter() throws Exception {
        final long base = 1;
        final long cap = 200;
        final boolean jitter = true;

        ExponentialWait waitStrategy = new ExponentialWait(base, cap, jitter);
        for(int i = 0; i < 500; i++){
            long actualWait = waitStrategy.getWait(i);

            Assert.assertTrue(actualWait >= 0);
            Assert.assertTrue(actualWait <= cap);

            if(isDebug)
                logger.debug("actualWait = {}", actualWait);
        }
    }
}
