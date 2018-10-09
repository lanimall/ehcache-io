package org.ehcache.extensions.io.impl.utils.extractors;

import net.sf.ehcache.Element;
import net.sf.ehcache.search.attribute.AttributeExtractor;
import net.sf.ehcache.search.attribute.AttributeExtractorException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMaster;

import java.util.Properties;

/**
 * Created by fabien.sanglier on 6/13/16.
 */
public class EhcacheStreamValueExtractor implements AttributeExtractor {
    private static final long serialVersionUID = 1L;

    public static final String FIELDNAME_MASTER_CHUNKCOUNT = "chunkCount";
    public static final String FIELDNAME_MASTER_WRITERS = "writers";
    public static final String FIELDNAME_MASTER_READERS = "readers";
    public static final String FIELDNAME_MASTER_LASTREADTIME = "lastRead";
    public static final String FIELDNAME_MASTER_LASTWRITTENTIME = "lastWritten";

    public static final Integer FIELDNAME_MASTER_CHUNKCOUNT_DEFAULT_NULL = new Integer(-1);
    public static final Integer FIELDNAME_MASTER_WRITERS_DEFAULT_NULL = new Integer(-1);
    public static final Integer FIELDNAME_MASTER_READERS_DEFAULT_NULL = new Integer(-1);
    public static final Long FIELDNAME_MASTER_LASTREADTIME_DEFAULT_NULL = new Long(-1L);
    public static final Long FIELDNAME_MASTER_LASTWRITETIME_DEFAULT_NULL = new Long(-1L);

    public EhcacheStreamValueExtractor(){}

    public EhcacheStreamValueExtractor(Properties properties) {
    }

    @Override
    public Object attributeFor(Element element, String attributeName) throws AttributeExtractorException {
        Object extracted;
        Object cacheValue = element.getObjectValue();

        if(FIELDNAME_MASTER_CHUNKCOUNT.equals(attributeName)){
            extracted = FIELDNAME_MASTER_CHUNKCOUNT_DEFAULT_NULL;
            if(null != cacheValue){
                if(cacheValue instanceof EhcacheStreamMaster)
                    extracted = new Integer(((EhcacheStreamMaster)cacheValue).getChunkCount());
            }
        } else if(FIELDNAME_MASTER_WRITERS.equals(attributeName)){
            extracted = FIELDNAME_MASTER_WRITERS_DEFAULT_NULL;
            if(null != cacheValue){
                if(cacheValue instanceof EhcacheStreamMaster)
                    extracted = new Integer(((EhcacheStreamMaster)cacheValue).getWriters());
            }
        } else if(FIELDNAME_MASTER_READERS.equals(attributeName)){
            extracted = FIELDNAME_MASTER_READERS_DEFAULT_NULL;
            if(null != cacheValue){
                if(cacheValue instanceof EhcacheStreamMaster)
                    extracted = new Integer(((EhcacheStreamMaster)cacheValue).getReaders());
            }
        } else if(FIELDNAME_MASTER_LASTREADTIME.equals(attributeName)){
            extracted = FIELDNAME_MASTER_LASTREADTIME_DEFAULT_NULL;
            if(null != cacheValue){
                if(cacheValue instanceof EhcacheStreamMaster)
                    extracted = new Long(((EhcacheStreamMaster)cacheValue).getLastReadTime());
            }
        } else if(FIELDNAME_MASTER_LASTWRITTENTIME.equals(attributeName)){
            extracted = FIELDNAME_MASTER_LASTWRITETIME_DEFAULT_NULL;
            if(null != cacheValue){
                if(cacheValue instanceof EhcacheStreamMaster)
                    extracted = new Long(((EhcacheStreamMaster)cacheValue).getLastWrittenTime());
            }
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Attribute name [%s] not supported by this extractor. Supported names are: [%s,%s,%s,%s,%s]",
                            attributeName,
                            FIELDNAME_MASTER_CHUNKCOUNT,
                            FIELDNAME_MASTER_WRITERS,
                            FIELDNAME_MASTER_READERS,
                            FIELDNAME_MASTER_LASTREADTIME,
                            FIELDNAME_MASTER_LASTWRITTENTIME)
            );
        }

        return extracted;
    }
}