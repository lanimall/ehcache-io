package org.ehcache.extensions.io.impl.writers;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamConcurrentException;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.EhcacheStreamTimeoutException;
import org.ehcache.extensions.io.impl.BaseEhcacheStream;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by fabien.sanglier on 7/24/18.
 */

/*
 * doc TBD
 */

/*package protected*/ class EhcacheStreamWriterNoLock extends BaseEhcacheStream implements EhcacheStreamWriter {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamWriterNoLock.class);
    private static final boolean isTrace = logger.isTraceEnabled();
    private static final boolean isDebug = logger.isDebugEnabled();

    private EhcacheStreamMaster oldStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;
    private final long openTimeoutMillis;

    public EhcacheStreamWriterNoLock(final Ehcache cache, final Object cacheKey, final boolean override, final long openTimeoutMillis) {
        super(cache, cacheKey);
        this.override = override;
        this.openTimeoutMillis = openTimeoutMillis;
    }

    public void tryOpen() throws EhcacheStreamException {
        if(openTimeoutMillis <= 0)
            throw new EhcacheStreamIllegalStateException(String.format("Open timeout [%d] may not be lower than 0", openTimeoutMillis));

        if (!isOpen) {
            try {
                // get the stream master for writing
                // if it's null, get out right away
                // if it's not null, check for available flag. if flag is not available for current writing,
                // retry some mor until either it is finally available for writing, OR it reaches the timeout
                long t1 = System.currentTimeMillis();
                long t2 = t1; //this ensures that the while always happen at least once!
                long itcounter = 0;
                while (!isOpen && t2 - t1 <= openTimeoutMillis) {
                    EhcacheStreamMaster initialStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
                    if(null == initialStreamMasterFromCache || null != initialStreamMasterFromCache && !initialStreamMasterFromCache.isCurrentWrite()) {
                        if(isTrace)
                            logger.trace("Got a stream master from cache that is now writable. Let's try to CAS it back in cache in write mode now.");

                        //if we're here, we've successfully acquired the lock -- otherwise, a EhcacheStreamException would have been thrown
                        //now, get the master index from cache, unless override is set
                        EhcacheStreamMaster newOpenStreamMaster = null;
                        if(null != initialStreamMasterFromCache) {
                            if (!override) {
                                newOpenStreamMaster = new EhcacheStreamMaster(initialStreamMasterFromCache.getChunkCount(), EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                            } else {
                                newOpenStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                            }
                        } else {
                            newOpenStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                        }

                        //concurrency check with CAS: let's save the initial EhcacheStreamMaster in cache, while making sure it hasn't change so far
                        //if multiple threads are trying to do this replace on same key, only one thread is guaranteed to succeed here...while others will fail their CAS ops...and spin back to try again later.
                        boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), initialStreamMasterFromCache, newOpenStreamMaster);
                        if (replaced) {
                            if(isDebug)
                                logger.trace("cas replace successful...Stream master is now saved in cache in write mode");

                            //if the new master has 0 chunk, it means it is an overwrite, so let's clear the chunks for the old master to keep things clean...
                            if (newOpenStreamMaster.getChunkCount() == 0) {
                                logger.trace("Clearing previous chunks ...open");
                                getEhcacheStreamUtils().clearChunksFromStreamMaster(getCacheKey(), initialStreamMasterFromCache);
                            }

                            //save (deep copy) the newOpenStreamMaster for later usage (for CAS replace) when we close
                            oldStreamMaster = EhcacheStreamMaster.deepCopy(newOpenStreamMaster);

                            //at this point, it's really open with consistency in cache
                            isOpen = true;
                            if(isDebug)
                                logger.trace("writer open successful...");
                        } else {
                            if(isTrace)
                                logger.trace("Could not cas replace the Stream master in cache, got beat by another thread. Let's retry in a bit.");
                        }
                    } else {
                        if(isTrace)
                            logger.trace("The current stream master in cache is marked as writable. Another thread must be working on it. Let's retry in a bit.");
                    }

                    try {
                        if(!isOpen) {
                            if(isTrace)
                                logger.trace("Sleeping before retry...");
                            Thread.sleep(PropertyUtils.DEFAULT_BUSYWAIT_RETRY_LOOP_SLEEP_MILLIS);
                        }
                    } catch (InterruptedException e) {
                        throw new EhcacheStreamException("Thread sleep interrupted", e);
                    } finally {
                        Thread.yield();
                    }

                    t2 = System.currentTimeMillis();
                    itcounter++;
                }

                if(isDebug)
                    logger.debug("Total cas loop iterations: {}", itcounter - 1);

                //if it's open opened at the end of all the tries and timeout, throw timeout exception
                if (!isOpen) {
                    throw new EhcacheStreamTimeoutException(String.format("Could not acquire a write after trying for %d ms (timeout triggers at %d ms)", t2 - t1, openTimeoutMillis));
                }
            } catch (Exception exc){
                isOpen = false;
                if(exc instanceof EhcacheStreamException)
                    throw exc;
                else
                    throw new EhcacheStreamException(exc);
            }
        }

        if (!isOpen || oldStreamMaster == null)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be opened at this point or an exception should have been thrown...something unexpected happened.");
    }

    @Override
    public void close() throws EhcacheStreamException {
        if(isOpen) {
            if (oldStreamMaster == null)
                throw new EhcacheStreamIllegalStateException("Trying to close, but EhcacheStreamMaster is null, which should never happen here. Something unexpected happened.");

            // finalize the EhcacheStreamMaster value by saving it in cache with available status --
            // this op must happen otherwise this entry will remain un-writeable / un-readable forever until manual cleanup
            boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), oldStreamMaster, oldStreamMaster.newWithStateChange(EhcacheStreamMaster.StreamOpStatus.AVAILABLE));
            if (!replaced)
                throw new EhcacheStreamConcurrentException("Could not close the ehcache stream index properly.");

            isOpen = false;
        }

        if (isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
    }

    /**
     * buf: The internal buffer where data is stored.
     * count: The number of valid bytes in the buffer. This value is always
     * in the range <tt>0</tt> through <tt>buf.length</tt>; elements
     * <tt>buf[0]</tt> through <tt>buf[count-1]</tt> contain valid
     * byte data.
     */
    public void writeData(byte[] buf, int count) throws EhcacheStreamException {
        if(!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter is not open...call open() first.");

        try {
            //get the stream master from cache (deep copy to make sure we don't use a reference to local cache
            EhcacheStreamMaster currentStreamMasterFromCache = EhcacheStreamMaster.deepCopy(getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey()));

            // check if the stream master we just got is the same as the one we opened with
            // this check should catch a currentStreamMasterFromCache = NULL too, since oldStreamMaster should not be null after open
            // currentStreamMasterFromCache = NULL should not happen, but if it does, make sure to throw exception (since the framework should not be able to delete the entry when it's write state)
            boolean isConsistent = EhcacheStreamMaster.compare(currentStreamMasterFromCache, oldStreamMaster);
            if(!isConsistent)
                throw new EhcacheStreamConcurrentException("Concurrent modification exception: EhcacheStreamMaster has changed since opening...concurrent write must have happened, and should not have."); //TODO: maybe a new EhcacheStreamConsistencyException?

            // now we know we are good with consistent EhcacheStreamMaster from cache, let's do the work.

            //put and increment stream index
            if(count > 0) {
                //copy and increment count
                EhcacheStreamMaster newIncrementedStreamMaster = currentStreamMasterFromCache.newWithIncrementCount();

                //TODO: maybe a local transaction here!!??
                //now, try to CAS replace the master (CAS barrier)
                boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), oldStreamMaster, newIncrementedStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamConcurrentException("Could not close the ehcache stream index properly.");

                // if multiple threads working on the same cache / cachekey, only 1 thread should reach here
                try {
                    // let's add the chunk (overwrite anything in cache)
                    getEhcacheStreamUtils().putChunkValue(getCacheKey(), oldStreamMaster.getChunkCount(), Arrays.copyOf(buf, count));

                    //now save the new master we just stored in cache to the instance for later compare
                    oldStreamMaster = newIncrementedStreamMaster;
                } catch (CacheException exc){
                    //if something happened during the addition of the chunk...try to revert as best we can
                    boolean replacedRollback = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), newIncrementedStreamMaster, oldStreamMaster);
                    if (!replacedRollback)
                        throw new EhcacheStreamConcurrentException(
                                String.format(
                                        "FATAL: An error happen while adding the chunk in cache, " +
                                                "and we could not rollback the increment of the stream master in cache..." +
                                                "The storage for master key [%s] is now inconsistent: Manual cleanup will be needed."
                                        , ((null != getCacheKey())?getCacheKey().toString():"null")
                                )
                        );
                    else
                        throw new EhcacheStreamException("An error happen while adding the chunk in cache", exc);
                }
            }
        } catch (Exception exc){
            throw new EhcacheStreamException(exc);
        }
    }
}
