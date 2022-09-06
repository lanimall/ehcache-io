package org.ehcache.extensions.io.impl.utils.cas;

import org.ehcache.extensions.io.EhcacheStreamingTestsBase;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamCasTest;
import org.ehcache.extensions.io.impl.utils.cas.ConstantWaitStrategy;
import org.ehcache.extensions.io.impl.utils.cas.ExponentialWait;
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

    private void debug(String message, Object... values){
        if(isDebug)
            logger.debug(message, values);
    }

    @Test
    public void testConstantWaitStrategy() throws Exception {
        long waitTime = 200;
        ConstantWaitStrategy waitStrategy = new ConstantWaitStrategy(waitTime);
        for(int i = 0; i < 500; i++){
            long actualWait = waitStrategy.getWait(i);
            Assert.assertEquals(waitTime, actualWait);

            debug("actualWait = {}", actualWait);
        }
    }

    @Test
    public void testExponentialWaitStrategyNoJitter() throws Exception {
        final long base = 1;
        final long cap = 100;
        final boolean jitter = false;

        ExponentialWait waitStrategy = new ExponentialWait(base, cap, jitter);
        for(int i = 0; i < 200; i++){
            long actualWait = waitStrategy.getWait(i);

            Assert.assertEquals(ExponentialWait.getWaitTimeNoJitter(cap, base, i), actualWait);
            Assert.assertTrue(actualWait > 0);
            Assert.assertTrue(actualWait <= cap);

            debug("Attempt = {} - actualWait = {}", i, actualWait);
        }
    }

    @Test
    public void testExponentialWaitStrategyWithJitter() throws Exception {
        final long base = 1;
        final long cap = 100;
        final boolean jitter = true;

        ExponentialWait waitStrategy = new ExponentialWait(base, cap, jitter);
        for(int i = 0; i < 200; i++){
            long actualWait = waitStrategy.getWait(i);

            Assert.assertTrue(actualWait >= 0);
            Assert.assertTrue(actualWait <= cap);

            debug("Attempt = {} - actualWait = {}", i, actualWait);
        }
    }
}
