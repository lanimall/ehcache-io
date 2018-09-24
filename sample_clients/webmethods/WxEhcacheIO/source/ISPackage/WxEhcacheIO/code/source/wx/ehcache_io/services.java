package wx.ehcache_io;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import java.io.*;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import org.ehcache.extensions.io.EhcacheIOStreams;
import com.wm.app.b2b.server.cache.CacheManagerUtil;
import com.wm.app.b2b.server.cache.config.CacheManagerConfig;
import java.util.List;
import java.util.zip.*;
// --- <<IS-END-IMPORTS>> ---

public final class services

{
	// ---( internal utility methods )---

	final static services _instance = new services();

	static services _newInstance() { return new services(); }

	static services _cast(Object o) { return (services)o; }

	// ---( server methods )---




	public static final void containsStreamEntry (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(containsStreamEntry)>> ---
		// @sigtype java 3.5
		// [i] field:0:required cacheManagerName
		// [i] field:0:required cacheName
		// [i] object:0:required key
		// [o] field:0:required found
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		String cacheManagerName = null;
		String cacheName = null;
		Object cacheKey = null;
		Cache cache = null;
		
		if (pipelineCursor.first("cacheManagerName"))
		{
			//get the filename string object out of the pipeline
			cacheManagerName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("cacheName"))
		{
			//get the filename string object out of the pipeline
			cacheName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("key"))
		{
			//get the filename string object out of the pipeline
			cacheKey = (String) pipelineCursor.getValue();
		}
		
		//get the cache
		cache = getCache(cacheManagerName, cacheName);
		
		boolean found = false;
		try {
			found = EhcacheIOStreams.containStreamEntry(cache, cacheKey);
		} catch (IOException e) {
			throw new ServiceException(e);
		}
		
		//output section
		pipelineCursor.last();
		
		//output the found flag
		IDataUtil.put(pipelineCursor, "found", new Boolean(found).toString().toLowerCase());
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



	public static final void getAsStream (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(getAsStream)>> ---
		// @sigtype java 3.5
		// [i] field:0:required cacheManagerName
		// [i] field:0:required cacheName
		// [i] object:0:required key
		// [i] field:0:optional decompress {"true","false"}
		// [o] object:0:required stream
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		String cacheManagerName = null;
		String cacheName = null;
		String decompressStr = null;
		Object cacheKey = null;
		Cache cache = null;
		
		if (pipelineCursor.first("cacheManagerName"))
		{
			//get the filename string object out of the pipeline
			cacheManagerName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("cacheName"))
		{
			//get the filename string object out of the pipeline
			cacheName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("decompress"))
		{
			//get the filename string object out of the pipeline
			decompressStr = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("key"))
		{
			//get the filename string object out of the pipeline
			cacheKey = (String) pipelineCursor.getValue();
		}
		
		//parse decompress value
		boolean decompress = (null != decompressStr && "true".equalsIgnoreCase(decompressStr));
		
		//force returning null on key not found
		boolean allowNullStream = true;
		
		//get the cache
		cache = getCache(cacheManagerName, cacheName);
		
		InputStream ehcacheInputStream = null;
		try {
			ehcacheInputStream = EhcacheIOStreams.getInputStream(cache, cacheKey, allowNullStream);
			if(null != ehcacheInputStream && ehcacheInputStream.available() > 0){
				if(decompress){
					ehcacheInputStream = new GZIPInputStream(ehcacheInputStream);
				}
			}
		} catch (IOException e) {
			//close the stream and nullify if there's an exception
			closeStream(ehcacheInputStream);
			ehcacheInputStream = null;
			
			throw new ServiceException(e);
		}
		
		//output section
		pipelineCursor.last();
		
		//output stream
		pipelineCursor.insertAfter("stream", ehcacheInputStream);
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



	public static final void getStreamEntryKeys (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(getStreamEntryKeys)>> ---
		// @sigtype java 3.5
		// [i] field:0:required cacheManagerName
		// [i] field:0:required cacheName
		// [i] field:0:optional excludeExpiredKeys {"true","false"}
		// [o] object:1:required keys
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		String cacheManagerName = null;
		String cacheName = null;
		Cache cache = null;
		String excludeExpiredKeysStr = null;
		
		if (pipelineCursor.first("cacheManagerName"))
		{
			//get the filename string object out of the pipeline
			cacheManagerName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("cacheName"))
		{
			//get the filename string object out of the pipeline
			cacheName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("excludeExpiredKeys"))
		{
			excludeExpiredKeysStr = (String) pipelineCursor.getValue();
		}
		
		//get the cache
		cache = getCache(cacheManagerName, cacheName);
		
		boolean excludeExpiredKeys = (null != excludeExpiredKeysStr && "true".equalsIgnoreCase(excludeExpiredKeysStr));
		
		List keys = null;
		try {
			keys = EhcacheIOStreams.getStreamEntryKeys(cache, excludeExpiredKeys);
		} catch (IOException e) {
			throw new ServiceException(e);
		}
		
		//output section
		pipelineCursor.last();
		
		//output the found flag
		IDataUtil.put(pipelineCursor, "keys", (null != keys)?keys.toArray():null);
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



	public static final void putAsStream (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(putAsStream)>> ---
		// @sigtype java 3.5
		// [i] field:0:required cacheManagerName
		// [i] field:0:required cacheName
		// [i] object:0:required key
		// [i] object:0:required stream
		// [i] field:0:optional append {"true","false"}
		// [i] field:0:optional compress {"true","false"}
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		InputStream inputStream = null;
		String cacheManagerName = null;
		String cacheName = null;
		String appendStr = null;
		String compressStr = null;
		Object cacheKey = null;
		Cache cache = null;
		
		if (pipelineCursor.first("cacheManagerName"))
		{
			//get the filename string object out of the pipeline
			cacheManagerName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("cacheName"))
		{
			//get the filename string object out of the pipeline
			cacheName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("key"))
		{
			//get the filename string object out of the pipeline
			cacheKey = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("append"))
		{
			//get the filename string object out of the pipeline
			appendStr = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("compress"))
		{
			//get the filename string object out of the pipeline
			compressStr = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("stream"))
		{
			Object o = pipelineCursor.getValue();
			if(null != o){
				if (o instanceof byte[])
				{
					byte[] buf = (byte[]) pipelineCursor.getValue();
					inputStream = new ByteArrayInputStream(buf);
				}
				else if (o instanceof InputStream)
				{
					inputStream = (InputStream) pipelineCursor.getValue();
				}
			}
		}
		
		boolean append = (null != appendStr && "true".equalsIgnoreCase(appendStr));
		boolean compress = (null != compressStr && "true".equalsIgnoreCase(compressStr));
		
		//get the cache
		cache = getCache(cacheManagerName, cacheName);
		
		if(null == inputStream)
			throw new ServiceException("InputStream provided is null...should not be.");
		
		OutputStream ehcacheOutputStream = null;
		try {
			ehcacheOutputStream = EhcacheIOStreams.getOutputStream(cache, cacheKey, !append);
			if(compress)
				ehcacheOutputStream = new GZIPOutputStream(ehcacheOutputStream);
		
			pipeStreamsWithBuffer(inputStream, ehcacheOutputStream, copyBufferSize);
		} catch (IOException e) {
			throw new ServiceException(e);
		} finally {
			closeStream(ehcacheOutputStream);
			ehcacheOutputStream = null;
		}
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



	public static final void removeStreamEntry (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(removeStreamEntry)>> ---
		// @sigtype java 3.5
		// [i] field:0:required cacheManagerName
		// [i] field:0:required cacheName
		// [i] object:0:required key
		// [o] field:0:required removed
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		String cacheManagerName = null;
		String cacheName = null;
		Object cacheKey = null;
		Cache cache = null;
		
		if (pipelineCursor.first("cacheManagerName"))
		{
			//get the filename string object out of the pipeline
			cacheManagerName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("cacheName"))
		{
			//get the filename string object out of the pipeline
			cacheName = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("key"))
		{
			//get the filename string object out of the pipeline
			cacheKey = (String) pipelineCursor.getValue();
		}
		
		//get the cache
		cache = getCache(cacheManagerName, cacheName);
		
		boolean removed = false;
		try {
			removed = EhcacheIOStreams.removeStreamEntry(cache, cacheKey);
		} catch (IOException e) {
			throw new ServiceException(e);
		}
		
		//output section
		pipelineCursor.last();
		
		//output deleted flag
		IDataUtil.put(pipelineCursor, "removed", new Boolean(removed).toString().toLowerCase());
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}

	// --- <<IS-START-SHARED>> ---
	static int copyBufferSize = 128*1024; //copy buffer
	
	public static void validateParams(String cacheManagerName) throws ServiceException {
		if (cacheManagerName == null || cacheManagerName.trim().length() == 0)
			throw new ServiceException("cacheManagerName may not be empty");
		else
			return;
	}
	
	public static void validateParams(String cacheManagerName, String cacheName) throws ServiceException {
		validateParams(cacheManagerName);
		if (cacheName == null || cacheName.trim().length() == 0)
			throw new ServiceException("cacheName may not be empty");
		else
			return;
	}
	
	public static CacheManager getCacheManager(String cacheManagerName) throws ServiceException {
		validateParams(cacheManagerName);
		CacheManager cacheManager = CacheManagerUtil.getCacheManager(cacheManagerName);
		if (null == cacheManager)
			throw new IllegalArgumentException("CacheManager is null...Check your configuration.");
	
		return cacheManager;
	}
	
	public static Cache getCache(String cacheManagerName, String cacheName) throws ServiceException {
		Cache cache = null;
		validateParams(cacheManagerName, cacheName);
		if (CacheManagerConfig.cacheManagerAlive(cacheManagerName)) {
			CacheManager cacheManager = getCacheManager(cacheManagerName);
			cache = cacheManager.getCache(cacheName);
			if (cache == null)
				throw new ServiceException("Cache " + cacheName + " is not found.");
		} else {
			throw new ServiceException("CacheManmager [" + cacheManagerName + "] is not started.");
		}
		return cache;
	}
	
	static void pipeStreamsByteByByte(InputStream is, OutputStream os) throws IOException {
	    int n;
	    while ((n = is.read()) > -1) {
	        os.write(n);
	    }
	}
	
	static void pipeStreamsWithBuffer(InputStream is, OutputStream os, int bufferSize) throws IOException {
	    int n;
	    byte[] buffer = new byte[bufferSize];
	    while ((n = is.read(buffer)) > -1) {
	        os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
	    }
	}
	
	static void closeStream(Closeable stream) throws ServiceException {
		if(null != stream){
			try {
				stream.close();
			} catch (IOException e) {
				throw new ServiceException(e);
			}
		}
	}
	// --- <<IS-END-SHARED>> ---
}

