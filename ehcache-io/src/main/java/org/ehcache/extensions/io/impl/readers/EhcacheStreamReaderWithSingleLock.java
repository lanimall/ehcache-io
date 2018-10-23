package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 7/24/18.
 */
/*package protected*/ class EhcacheStreamReaderWithSingleLock extends BaseEhcacheStreamReader implements EhcacheStreamReader {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamReaderWithSingleLock.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    private volatile boolean isOpenLockAcquired = false;

    private final long openTimeoutMillis;

    public EhcacheStreamReaderWithSingleLock(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
        super(cache, cacheKey);

        if(openTimeoutMillis <= 0)
            throw new EhcacheStreamIllegalStateException(String.format("Open timeout [%d] may not be lower than 0", openTimeoutMillis));

        this.openTimeoutMillis = openTimeoutMillis;
    }

    @Override
    public void oneTimeInit() throws EhcacheStreamException {
        if(isDebug)
            logger.debug("In oneTimeInit for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

        getEhcacheStreamUtils().acquireReadOnMaster(getPublicCacheKey(), openTimeoutMillis);

        isOpenLockAcquired = true;

        activeStreamMaster = EhcacheStreamMaster.deepCopy(
                getEhcacheStreamUtils().getStreamMasterFromCache(getPublicCacheKey())
        );

        if(isDebug)
            logger.debug("Opened reader for key={} is {}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()), EhcacheStreamUtilsInternal.toStringSafe(activeStreamMaster));
    }

    @Override
    void oneTimeCleanup() throws EhcacheStreamException {
        if(isDebug)
            logger.debug("In oneTimeCleanup for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

        try {
            if (isOpenLockAcquired) {
                //release the lock
                getEhcacheStreamUtils().releaseReadOnMaster(getPublicCacheKey());
            }
        } finally {
            //clean the internal vars
            isOpenLockAcquired = false;
        }
    }

    @Override
    public int read(byte[] outBuf, int bufferBytePos, int len) throws EhcacheStreamException {
        checkIfOpen();

        try {
            // copy the cache chunks into the buffer based on the internal index tracker
            return read1(outBuf, bufferBytePos, len);
        } catch (EhcacheStreamIllegalStateException exc){
            throw new EhcacheStreamIllegalStateException(String.format("Could not read the cache chunk. Current StreamMaster[=%s]", EhcacheStreamUtilsInternal.toStringSafe(activeStreamMaster)), exc);
        }
    }
}
