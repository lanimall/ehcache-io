package org.ehcache.extensions.io.impl;

import net.sf.ehcache.Cache;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
public class EhcacheStreamWriter extends BaseEhcacheStream {

    public EhcacheStreamWriter(Cache cache, Object cacheKey) {
        super(cache, cacheKey);
    }

    public void open() throws InterruptedException {
        synchronized (this.getClass()) {
            acquireExclusiveWrite(LOCK_TIMEOUT);
        }
    }

    public void close() {
        synchronized (this.getClass()) {
            //finalize the EhcacheStreamMaster value by saving it in cache
            EhcacheStreamMaster currentStreamMaster = getMasterIndexValue();
            if(null != currentStreamMaster) {
                currentStreamMaster.setAvailable();
                replaceEhcacheStreamMaster(currentStreamMaster);
            }
            releaseExclusiveWrite();
        }
    }

    /**
     * buf: The internal buffer where data is stored.
     * count: The number of valid bytes in the buffer. This value is always
     * in the range <tt>0</tt> through <tt>buf.length</tt>; elements
     * <tt>buf[0]</tt> through <tt>buf[count-1]</tt> contain valid
     * byte data.
     */
    public void writeData(byte[] buf, int count) throws InterruptedException, IOException {
        synchronized (this.getClass()) {
            //first we try to acquire the write lock
            acquireExclusiveWrite(LOCK_TIMEOUT);

            //then we get the index to know where we are in the writes
            EhcacheStreamMaster currentStreamMaster = getMasterIndexValue();
            if (null == currentStreamMaster) {
                //set a new EhcacheStreamMasterIndex in write mode
                EhcacheStreamMaster newStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);

                boolean replaced = replaceEhcacheStreamMaster(null, newStreamMaster);
                if(!replaced)
                    throw new IOException("Concurrent write not allowed - Current cache entry with key[" + cacheKey + "] is currently being written...");

                //if previous cas operation successful, create a new EhcacheStreamMasterIndex for currentStreamMasterIndex (to avoid soft references issues to the cached value above)
                currentStreamMaster = new EhcacheStreamMaster(EhcacheStreamMaster.StreamOpStatus.CURRENT_WRITE);

                //at this point, a new master entry is created -- let's do some cleanup first
                clearAllChunks();
            }

            //TODO we probably should do both of these in a local transaction to ensure consistency
            putChunkValue(currentStreamMaster.getAndIncrementChunkIndex(), Arrays.copyOf(buf, count));
            replaceEhcacheStreamMaster(currentStreamMaster);
        }
    }
}
