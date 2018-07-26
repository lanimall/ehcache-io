package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
public class EhcacheStreamWriter extends BaseEhcacheStream implements Closeable {

    /*
     * The current position in the ehcache value chunk list.
     */
    private volatile EhcacheStreamMaster currentStreamMaster;

    private final boolean override;

    private volatile boolean isOpen = false;

    public EhcacheStreamWriter(Cache cache, Object cacheKey, boolean override) {
        super(cache, cacheKey);
        this.override = override;
    }

    public void open() throws IOException {
        if(!isOpen) {
            synchronized (this.getClass()) {
                if (!isOpen) {
                    try {
                        acquireExclusiveWriteOnMaster(LOCK_TIMEOUT);
                    } catch (InterruptedException e) {
                        throw new IOException("Could not acquire the internal ehcache write lock", e);
                    }

                    //get the master index from cache, unless override is set
                    Element oldMasterIndexElement = getMasterIndexElement();
                    EhcacheStreamMaster oldMasterIndex = null;
                    if(null != oldMasterIndexElement) {
                        oldMasterIndex = (EhcacheStreamMaster)oldMasterIndexElement.getObjectValue();
                        if (!override) {
                            this.currentStreamMaster = oldMasterIndex;
                            //maybe set it to write in the cache...
                        } else {
                            this.currentStreamMaster = null;
                        }
                    }

                    //then we get the index to know where we are in the writes
                    if (null == currentStreamMaster) {
                        //set a new EhcacheStreamMasterIndex in write mode
                        EhcacheStreamMaster newStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);

                        boolean replaced = replaceEhcacheStreamMaster(oldMasterIndexElement, newStreamMaster);
                        if(!replaced)
                            throw new IOException("Concurrent write not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");

                        //if previous cas operation successful, create a new EhcacheStreamMasterIndex for currentStreamMasterIndex (to avoid soft references issues to the cached value above)
                        currentStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);

                        //clear the chunks for the old master...
                        clearChunksForKey(oldMasterIndex);
                    }

                    isOpen = true;
                }
            }
        }
    }

    public void close() throws IOException {
        if(!isOpen)
            throw new IOException("EhcacheStreamWriter is not open...");

        synchronized (this.getClass()) {
            if(!isOpen)
                throw new IOException("EhcacheStreamWriter is not open...");

            //finalize the EhcacheStreamMaster value by saving it in cache
            //EhcacheStreamMaster currentStreamMaster = getMasterIndexValue();
            if (null != currentStreamMaster) {
                currentStreamMaster.setAvailable();
                if(!replaceEhcacheStreamMaster(currentStreamMaster))
                    throw new IOException("Could not close the ehcache stream index properly.");
            }
            releaseExclusiveWriteOnMaster();
            isOpen = false;
        }
    }

    /**
     * buf: The internal buffer where data is stored.
     * count: The number of valid bytes in the buffer. This value is always
     * in the range <tt>0</tt> through <tt>buf.length</tt>; elements
     * <tt>buf[0]</tt> through <tt>buf[count-1]</tt> contain valid
     * byte data.
     */
    public void writeData(byte[] buf, int count) throws IOException {
        if(!isOpen)
            throw new IllegalStateException("EhcacheStreamWriter is not open...call open() first.");

        //put and increment stream index
        if(count > 0) {
            putChunkValue(currentStreamMaster.getAndIncrementChunkCounter(), Arrays.copyOf(buf, count));
        }
    }
}
