package org.ehcache.extensions.io.impl;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamConcurrentException;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Created by fabien.sanglier on 7/24/18.
 */

/*
 * doc TBD
 */

/*package protected*/ class EhcacheStreamWriterNoLock extends BaseEhcacheStream implements Closeable {

    private EhcacheStreamMaster oldStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;

    public EhcacheStreamWriterNoLock(Ehcache cache, Object cacheKey, boolean override) {
        super(cache, cacheKey);
        this.override = override;
    }

    public void tryOpen(final long timeoutMillis) throws EhcacheStreamException {
        if (!isOpen) {
            try {
                //TODO: sync protect that loop?
                // get the stream master for writing
                // if it's null, get out right away
                // if it's not null, check for available flag. if flag is not available for current writing, try a couple of time with some wait
                EhcacheStreamMaster initialStreamMasterFromCache = null;
                long t1 = System.currentTimeMillis();
                long t2 = t1; //this ensures that the while always happen at least once!
                while (t2 - t1 <= timeoutMillis) {
                    initialStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
                    if(null == initialStreamMasterFromCache || null != initialStreamMasterFromCache && !initialStreamMasterFromCache.isCurrentWrite())
                        break;

                    try {
                        Thread.sleep(50L);
                    } catch (InterruptedException e) {
                        throw new EhcacheStreamException("Thread sleep interrupted", e);
                    } finally {
                        Thread.yield();
                    }
                    t2 = System.currentTimeMillis();
                }

                //if the stream master is still being written at the end of the tries, stop trying...
                if (null != initialStreamMasterFromCache && initialStreamMasterFromCache.isCurrentWrite()) {
                    throw new EhcacheStreamException(String.format("Could not acquire a write within timeout %d ms.", t2 - t1));
                }

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
                //if multiple threads are trying to do this replace on same key, only one thread is guaranteed to succeed...others will end up throwing a EhcacheStreamException
                boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), initialStreamMasterFromCache, newOpenStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamConcurrentException("Concurrent write not allowed - Current cache entry with key[" + getCacheKey() + "] is currently being written...");

                //if the new master has 0 chunk, it means it is an overwrite, so let's clear the chunks for the old master to keep things clean...
                if(newOpenStreamMaster.getChunkCount() == 0)
                    getEhcacheStreamUtils().clearChunksFromStreamMaster(getCacheKey(), initialStreamMasterFromCache);

                //save (deep copy) the newOpenStreamMaster for later usage (for CAS replace) when we close
                oldStreamMaster = EhcacheStreamMaster.deepCopy(newOpenStreamMaster);

                //at this point, it's really open with consistency in cache
                isOpen = true;
            } catch (Exception exc){
                isOpen = false;
                throw new EhcacheStreamException(exc);
            }
        }

        if (!isOpen || oldStreamMaster == null)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be opened at this point: something unexpected happened.");
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

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}
