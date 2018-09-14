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
/*package protected*/ class EhcacheStreamWriterWithSingleLock extends BaseEhcacheStream implements Closeable {

    private EhcacheStreamMaster initialOpenedStreamMaster;
    private EhcacheStreamMaster currentStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;

    public EhcacheStreamWriterWithSingleLock(Ehcache cache, Object cacheKey, boolean override) {
        super(cache, cacheKey);
        this.override = override;
    }

    public void tryOpen(long timeout) throws EhcacheStreamException {
        if (!isOpen) {
            //always try to acquire the lock first
            getEhcacheStreamUtils().acquireExclusiveWriteOnMaster(getCacheKey(), timeout);

            //if we're here, we've successfully acquired the lock -- otherwise, a EhcacheStreamException would have been thrown
            //now, get the master index from cache, unless override is set
            EhcacheStreamMaster oldStreamMaster = null;
            try {
                oldStreamMaster = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());

                EhcacheStreamMaster newOpenStreamMaster = null;
                if(null != oldStreamMaster) {
                    if (!override) {
                        newOpenStreamMaster = new EhcacheStreamMaster(oldStreamMaster.getChunkCount(), EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                    } else {
                        newOpenStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                    }
                } else {
                    newOpenStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                }

                boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), oldStreamMaster, newOpenStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamConcurrentException("Concurrent write not allowed - Current cache entry with key[" + getCacheKey() + "] is currently being written...");

                //save (deep copy) the newOpenStreamMaster for later usage (for CAS replace) when we close
                this.initialOpenedStreamMaster = EhcacheStreamMaster.deepCopy(newOpenStreamMaster);

                //clear the chunks for the old master to keep things clean...
                if(null != oldStreamMaster && newOpenStreamMaster.getChunkCount() == 0)
                    getEhcacheStreamUtils().clearChunksFromStreamMaster(getCacheKey(), oldStreamMaster);

                //if the previous cas operation was successful, save (deep copy) the value from the cache into a instance variable
                EhcacheStreamMaster ehcacheStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
                this.currentStreamMaster = EhcacheStreamMaster.deepCopy(ehcacheStreamMasterFromCache);

                //at this point, it's really open with consistency in cache
                isOpen = true;
            } catch (Exception exc){
                //release lock
                getEhcacheStreamUtils().releaseExclusiveWriteOnMaster(getCacheKey());
                isOpen = false;
                throw exc;
            }
        }

        if (!isOpen)
            throw new EhcacheStreamIllegalStateException("EhcacheStreamWriter should be opened at this point: something unexpected happened.");
    }

    @Override
    public void close() throws EhcacheStreamException {
        if(isOpen) {
            if (currentStreamMaster == null)
                throw new EhcacheStreamIllegalStateException("Trying to close, but EhcacheStreamMaster is null, which should never happen here. Something unexpected happened.");

            boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), initialOpenedStreamMaster, currentStreamMaster.newWithStateChange(EhcacheStreamMaster.StreamOpStatus.AVAILABLE));
            if (!replaced)
                throw new EhcacheStreamConcurrentException("Could not close the ehcache stream index properly.");

            //release the lock
            getEhcacheStreamUtils().releaseExclusiveWriteOnMaster(getCacheKey());

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

        //put and increment stream index
        if(count > 0) {
            //copy and increment count
            EhcacheStreamMaster newIncrementedStreamMaster = currentStreamMaster.newWithIncrementCount();

            getEhcacheStreamUtils().putChunkValue(getCacheKey(), newIncrementedStreamMaster.getChunkCount(), Arrays.copyOf(buf, count));
        }
    }
}
