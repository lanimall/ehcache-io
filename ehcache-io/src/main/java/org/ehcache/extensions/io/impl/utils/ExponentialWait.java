package org.ehcache.extensions.io.impl.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by fabien.sanglier on 9/18/18.
 */
public class ExponentialWait implements WaitStrategy {
    private static final Logger logger = LoggerFactory.getLogger(ExponentialWait.class);
    private static final boolean isTrace = logger.isTraceEnabled();

    public static final boolean DEFAULT_WAIT_USE_JITTER = true;

    private final long base;
    private final long cap;
    private final boolean jitter;

    public ExponentialWait(long base, long cap) {
        this(base, cap, DEFAULT_WAIT_USE_JITTER);
    }

    public ExponentialWait(long base, long cap, boolean jitter) {
        this.base = base;
        this.cap = cap;
        this.jitter = jitter;
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
        return jitter ? getWaitTimeWithJitter(cap, base, attempt) : getWaitTimeNoJitter(cap, base, attempt);
    }

    protected static long getWaitTimeNoJitter(final long cap, final long base, final long n) {
        // Simple check for overflows
        final long expWait = ((long) Math.pow(2, n)) * base;
        return expWait <= 0 ? cap : Math.min(cap, expWait);
    }

    protected static long getWaitTimeWithJitter(final long cap, final long base, final long n) {
        return ThreadLocalRandom.current().nextLong(0, getWaitTimeNoJitter(cap, base, n));
    }
}
