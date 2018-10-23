package org.ehcache.extensions.io.impl.readers;

import net.sf.ehcache.Ehcache;
import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.EhcacheStreamIllegalStateException;
import org.ehcache.extensions.io.impl.utils.EhcacheStreamUtilsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fabien.sanglier on 7/24/18.
 */

/*
 * doc TBD
 */

/*package protected*/ class EhcacheStreamReaderCasLock extends BaseEhcacheStreamReader implements EhcacheStreamReader {
    private static final Logger logger = LoggerFactory.getLogger(EhcacheStreamReaderCasLock.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    private volatile boolean isOpenMasterMutated = false;

    private final long openTimeoutMillis;

    public EhcacheStreamReaderCasLock(Ehcache cache, Object cacheKey, long openTimeoutMillis) {
        super(cache, cacheKey);

        if(openTimeoutMillis <= 0)
            throw new EhcacheStreamIllegalStateException(String.format("Open timeout [%d] may not be lower than 0", openTimeoutMillis));

        this.openTimeoutMillis = openTimeoutMillis;
    }

    @Override
    void oneTimeInit() throws EhcacheStreamException {
        if(isDebug)
            logger.debug("In oneTimeInit for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

        activeStreamMaster = getEhcacheStreamUtils().openReadOnMaster(
                getPublicCacheKey(),
                openTimeoutMillis
        );

        if(isDebug)
            logger.debug("Opened reader for key={} is {}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()), EhcacheStreamUtilsInternal.toStringSafe(activeStreamMaster));

        //EhcacheStreamReader could be null if the entry was not found in cache
        if(null != activeStreamMaster) {
            if(activeStreamMaster.getReaders() == 0)
                throw new EhcacheStreamIllegalStateException("EhcacheStreamReader should not have 0 reader at this point");

            //we want to make sure that we only close the master if it was mutated properly here (otherwise, the read counts would not be accurate)
            //so let's mark here that we're mutated properly...and use that flag in the close
            isOpenMasterMutated = true;
        }
    }

    @Override
    void oneTimeCleanup() throws EhcacheStreamException {
        if(isDebug)
            logger.debug("In oneTimeCleanup for key={}", EhcacheStreamUtilsInternal.toStringSafe(getPublicCacheKey()));

        try {
            // finalize the EhcacheStreamMaster value by saving it in cache with reader count decremented --
            // this op must happen otherwise this entry will remain un-writeable forever until manual cleanup
            if (isOpenMasterMutated) {
                getEhcacheStreamUtils().closeReadOnMaster(
                        getPublicCacheKey(),
                        openTimeoutMillis
                );
            }
        } finally {
            //clean the internal vars
            isOpenMasterMutated = false;
        }
    }

    @Override
    public int read(byte[] outBuf, int off, int len) throws EhcacheStreamException {
        checkIfOpen();

        try {
            // copy the cache chunks into the buffer based on the internal index tracker
            return read1(outBuf, off, len);
        } catch (EhcacheStreamIllegalStateException exc){
            throw new EhcacheStreamIllegalStateException(String.format("Could not read the cache chunk. Current StreamMaster[=%s]", EhcacheStreamUtilsInternal.toStringSafe(activeStreamMaster)), exc);
        }
    }
}
