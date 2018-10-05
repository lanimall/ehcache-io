package org.ehcache.extensions.io.impl.utils.extractors;

import net.sf.ehcache.Element;
import net.sf.ehcache.search.attribute.AttributeExtractor;
import net.sf.ehcache.search.attribute.AttributeExtractorException;
import org.ehcache.extensions.io.impl.model.EhcacheStreamChunkKey;
import org.ehcache.extensions.io.impl.model.EhcacheStreamMasterKey;

import java.util.Properties;

/**
 * Created by fabien.sanglier on 6/13/16.
 */
public class EhcacheStreamKeyExtractor implements AttributeExtractor {
    private static final long serialVersionUID = 1L;

    public static final String FIELDNAME_CACHEKEYTYPE = "keyType";
    public static final String FIELDNAME_CACHEKEY = "publicKey";
    public static final String FIELDNAME_CHUNKINDEX = "keyChunkIndex";

    public static final String FIELDNAME_CACHEKEY_DEFAULT_NULL = "";

    public static final Integer FIELDNAME_CHUNKINDEX_DEFAULT_NULL = new Integer(-1);

    public static final Integer FIELDNAME_CACHEKEYTYPE_DEFAULT_NULL = new Integer(-1);
    public static final Integer FIELDNAME_CACHEKEYTYPE_MASTERKEY = new Integer(0);
    public static final Integer FIELDNAME_CACHEKEYTYPE_CHUNKKEY = new Integer(1);
    public static final Integer FIELDNAME_CACHEKEYTYPE_OTHER = new Integer(2); //this is more to verify nothing else is coming into the cache

    public EhcacheStreamKeyExtractor(Properties properties) {
    }

    @Override
    public Object attributeFor(Element element, String attributeName) throws AttributeExtractorException {
        Object extracted;
        Object cacheKey = element.getObjectKey();
        if(FIELDNAME_CACHEKEY.equals(attributeName)){
            extracted = FIELDNAME_CACHEKEY_DEFAULT_NULL;
            if(null != cacheKey){
                if(cacheKey instanceof EhcacheStreamMasterKey) //this will include master or chunk key
                    extracted = ((EhcacheStreamMasterKey)cacheKey).getCacheKey().toString(); //the problem here is that if it is a complex object, it won't make much sense to index that...
            }
        } else if(FIELDNAME_CHUNKINDEX.equals(attributeName)){
            extracted = FIELDNAME_CHUNKINDEX_DEFAULT_NULL;
            if(null != cacheKey){
                if(cacheKey instanceof EhcacheStreamChunkKey)
                    extracted = new Integer(((EhcacheStreamChunkKey)cacheKey).getChunkIndex());
            }
        } else if(FIELDNAME_CACHEKEYTYPE.equals(attributeName)){
            extracted = FIELDNAME_CACHEKEYTYPE_DEFAULT_NULL;
            if(null != cacheKey){
                if(cacheKey instanceof EhcacheStreamChunkKey)
                    extracted = FIELDNAME_CACHEKEYTYPE_CHUNKKEY;
                else if(cacheKey instanceof EhcacheStreamMasterKey)
                    extracted = FIELDNAME_CACHEKEYTYPE_MASTERKEY;
                else
                    extracted = FIELDNAME_CACHEKEYTYPE_OTHER;
            }
        } else {
            throw new IllegalStateException(String.format("Attribute name [%s] not supported by this extractor. Supported names are: [%s,%s]", attributeName, FIELDNAME_CACHEKEY, FIELDNAME_CHUNKINDEX));
        }

        return extracted;
    }
}