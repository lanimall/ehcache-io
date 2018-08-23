package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheIOStreams;
import org.ehcache.extensions.io.EhcacheStreamException;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
/*package protected*/ class EhcacheStreamWriter extends BaseEhcacheStream implements Closeable {

    /*
     * The current position in the ehcache value chunk list.
     */
    private volatile EhcacheStreamMaster currentStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;

    public EhcacheStreamWriter(Ehcache cache, Object cacheKey, boolean override) {
        super(cache, cacheKey);
        this.override = override;
    }

    public void tryOpen(long timeout) throws EhcacheStreamException {
        if(!isOpen) {
            synchronized (this.getClass()) {
                if (!isOpen) {
                    getEhcacheStreamUtils().acquireExclusiveWriteOnMaster(getCacheKey(), timeout);

                    //get the master index from cache, unless override is set
                    EhcacheStreamMaster oldStreamMaster = getEhcacheStreamUtils().getStreamMasterFromCache(getCacheKey());
                    if(null != oldStreamMaster) {
                        if (!override) {
                            this.currentStreamMaster = oldStreamMaster;
                            //maybe set it to write in the cache...
                        } else {
                            this.currentStreamMaster = null;
                        }
                    }

                    //If current stream master is null, create a new one
                    if (null == currentStreamMaster) {
                        //set a new EhcacheStreamMasterIndex in write mode
                        EhcacheStreamMaster newStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);

                        boolean replaced = getEhcacheStreamUtils().replaceIfEqualEhcacheStreamMaster(getCacheKey(), oldStreamMaster, newStreamMaster);
                        if(!replaced)
                            throw new EhcacheStreamException("Concurrent write not allowed - Current cache entry with key[" + getCacheKey() + "] is currently being written...");

                        //if previous cas operation successful, create a new EhcacheStreamMasterIndex for currentStreamMasterIndex (to avoid soft references issues to the cached value above)
                        currentStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);

                        //clear the chunks for the old master...
                        getEhcacheStreamUtils().clearChunksFromStreamMaster(getCacheKey(), oldStreamMaster);
                    }

                    isOpen = true;
                }
            }
        }

        if (!isOpen)
            throw new EhcacheStreamException("EhcacheStreamWriter should be opened at this point: something unexpected happened.");
    }

    public void close() throws EhcacheStreamException {
        if(isOpen) {
            synchronized (this.getClass()) {
                if(isOpen) {
                    //finalize the EhcacheStreamMaster value by saving it in cache
                    //EhcacheStreamMaster currentStreamMaster = getStreamMasterFromCache();
                    if (null != currentStreamMaster) {
                        currentStreamMaster.setAvailable();
                        if (!getEhcacheStreamUtils().replaceIfPresentEhcacheStreamMaster(getCacheKey(), currentStreamMaster))
                            throw new EhcacheStreamException("Could not close the ehcache stream index properly.");
                    }
                    getEhcacheStreamUtils().releaseExclusiveWriteOnMaster(getCacheKey());
                    isOpen = false;
                }
            }
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
