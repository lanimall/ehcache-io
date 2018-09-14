package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
/*package protected*/ class EhcacheStreamWriter extends BaseEhcacheStream implements Closeable {

    private EhcacheStreamMaster initialOpenedStreamMaster;
    private EhcacheStreamMaster currentStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;

    public EhcacheStreamWriter(Ehcache cache, Object cacheKey, boolean override) {
        super(cache, cacheKey);
        this.override = override;
    }

    public void tryOpen(final long timeout) throws EhcacheStreamException {
        if (!isOpen) {
            //always try to acquire the lock first
            if(!EhcacheStreamUtils.outputStreamDontUseWriteLock)
                getEhcacheStreamUtils().acquireExclusiveWriteOnMaster(getCacheKey(), timeout);

            //if we're here, we've successfully acquired the lock -- otherwise, a EhcacheStreamException would have been thrown
            //now, get the master index from cache, unless override is set
            EhcacheStreamMaster oldStreamMaster = null;
            try {
                oldStreamMaster = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());

                EhcacheStreamMaster newOpenStreamMaster = null;
                if(null != oldStreamMaster) {
                    if (!override) {
                        newOpenStreamMaster = new EhcacheStreamMaster(oldStreamMaster.getChunkCounter(), EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                    } else {
                        newOpenStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                    }
                } else {
                    newOpenStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);
                }

                boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), oldStreamMaster, newOpenStreamMaster);
                if (!replaced)
                    throw new EhcacheStreamException("Concurrent write not allowed - Current cache entry with key[" + getCacheKey() + "] is currently being written...");

                //save (deep copy) the newOpenStreamMaster for later usage (for CAS replace) when we close
                this.initialOpenedStreamMaster = new EhcacheStreamMaster(newOpenStreamMaster);

                //clear the chunks for the old master to keep things clean...
                if(null != oldStreamMaster && newOpenStreamMaster.getChunkCounter() == 0)
                    getEhcacheStreamUtils().clearChunksFromStreamMaster(getCacheKey(), oldStreamMaster);

                //if the previous cas operation was successful, save (deep copy) the value from the cache into a instance variable
                EhcacheStreamMaster ehcacheStreamMasterFromCache = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
                this.currentStreamMaster = (null != ehcacheStreamMasterFromCache)?new EhcacheStreamMaster(ehcacheStreamMasterFromCache):null;

                //at this point, it's really open with consistency in cache
                isOpen = true;
            } catch (Exception exc){
                //release lock
                if(!EhcacheStreamUtils.outputStreamDontUseWriteLock)
                    getEhcacheStreamUtils().releaseExclusiveWriteOnMaster(getCacheKey());
                isOpen = false;
                throw exc;
            }
        }

        if (!isOpen)
            throw new EhcacheStreamException("EhcacheStreamWriter should be opened at this point: something unexpected happened.");
    }

    @Override
    public void close() throws EhcacheStreamException {
        if(isOpen) {
            //finalize the EhcacheStreamMaster value by saving it in cache
            EhcacheStreamMaster finalStreamMaster;
            if (null != currentStreamMaster) {
                finalStreamMaster = new EhcacheStreamMaster(currentStreamMaster.getChunkCounter(), EhcacheStreamMaster.StreamOpStatus.AVAILABLE);
            } else {
                finalStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.AVAILABLE);
            }

            boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), initialOpenedStreamMaster, finalStreamMaster);
            if (!replaced)
                throw new EhcacheStreamException("Could not close the ehcache stream index properly.");

            if(!EhcacheStreamUtils.outputStreamDontUseWriteLock)
                getEhcacheStreamUtils().releaseExclusiveWriteOnMaster(getCacheKey());

            isOpen = false;
        }

        if (isOpen)
            throw new EhcacheStreamException("EhcacheStreamWriter should be closed at this point: something unexpected happened.");
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
            throw new IllegalStateException("EhcacheStreamWriter is not open...call open() first.");

        //put and increment stream index
        if(count > 0) {
            getEhcacheStreamUtils().putChunkValue(getCacheKey(), currentStreamMaster.getAndIncrementChunkCounter(), Arrays.copyOf(buf, count));
        }
    }
}
