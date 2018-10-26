package org.ehcache.extensions.io.impl.utils.cas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 9/18/18.
 */
public class ConstantWaitStrategy implements WaitStrategy {
    private static final Logger logger = LoggerFactory.getLogger(ConstantWaitStrategy.class);
    private static final boolean isTrace = logger.isTraceEnabled();

    private final long waitTime;

    public ConstantWaitStrategy(long waitTime) {
        this.waitTime = waitTime;
    }

    public void doWait(final long attempt) {
        final long waitTime = getWait(attempt);
        if(isTrace) logger.trace("Attempt #{}: will wait {} ms", attempt, waitTime);

        try {
            Thread.sleep(waitTime);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected long getWait(final long attempt) {
        return waitTime;
    }
}
