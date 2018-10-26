package wx.ehcache_io;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import java.io.*;
import net.sf.ehcache.Ehcache;
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




	public static final void appendToStream (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(appendToStream)>> ---
		// @sigtype java 3.5
		// [i] object:0:required bytesOrInputStream
		// [i] object:0:required outputStream
		// [i] field:0:required copyBufferSize
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		OutputStream outputStream = null;
		InputStream inputStream = null;
		
		//input stream
		if (pipelineCursor.first("bytesOrInputStream"))
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
		
		if(null == inputStream)
			throw new ServiceException("inputStream provided is null...should not be.");
		
		//output stream
		if (pipelineCursor.first("outputStream"))
		{
			outputStream = (OutputStream)pipelineCursor.getValue();
		}
		
		if (outputStream == null)
			throw new ServiceException("outputStream provided is null...should not be.");
			
		//stream copy buffer size
		Integer copyBufferSize = null;
		if (pipelineCursor.first("copyBufferSize"))
		{
			String copyBufferSizeStr = (String)pipelineCursor.getValue();
			try{
				if(null != copyBufferSizeStr && !"".equals(copyBufferSizeStr))
					copyBufferSize = Integer.parseInt(copyBufferSizeStr);
			} catch (NumberFormatException nfe){
				copyBufferSize = null;
			}	
		}
		
		//create the OutputStream and copy to it directly
		try {
			if(copyBufferSize != null)
				pipeStreamsWithBuffer(inputStream, outputStream, copyBufferSize);
			else 
				pipeStreamsByteByByte(inputStream, outputStream);
		} catch (IOException e) {
			throw new ServiceException(e);
		}
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



	public static final void closeStream (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(closeStream)>> ---
		// @sigtype java 3.5
		// [i] object:0:required stream
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		Closeable stream = null;
		if (pipelineCursor.first("stream"))
		{
			try{
				stream = (Closeable)pipelineCursor.getValue();
			} catch (ClassCastException cce){
				throw new ServiceException("stream object does not implement Closeable...should not be.");
			}
		}
		
		//close the stream
		closeStream(stream);
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



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
		
		//get cache handle
		Ehcache cache = getCache(pipelineCursor);
		
		Object cacheKey = null;
		if (pipelineCursor.first("key"))
		{
			//get the filename string object out of the pipeline
			cacheKey = (String) pipelineCursor.getValue();
		}
		
		boolean found = EhcacheIOStreams.containStreamEntry(cache, cacheKey);
		
		//output section
		pipelineCursor.last();
		
		//output the found flag
		IDataUtil.put(pipelineCursor, "found", new Boolean(found).toString().toLowerCase());
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



	public static final void createGetStream (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(createGetStream)>> ---
		// @sigtype java 3.5
		// [i] field:0:required cacheManagerName
		// [i] field:0:required cacheName
		// [i] object:0:required key
		// [i] field:0:optional decompress {"true","false"}
		// [i] field:0:required bufferSize
		// [i] field:0:required openTimeout
		// [o] object:0:required ehcacheInputStream
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		InputStream ehcacheInputStream = createInputStream(pipelineCursor);
		
		//output section
		pipelineCursor.last();
		
		//output stream
		pipelineCursor.insertAfter("ehcacheInputStream", ehcacheInputStream);
		
		pipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



	public static final void createPutStream (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(createPutStream)>> ---
		// @sigtype java 3.5
		// [i] field:0:required cacheManagerName
		// [i] field:0:required cacheName
		// [i] object:0:required key
		// [i] field:0:optional append {"true","false"}
		// [i] field:0:optional compress {"true","false"}
		// [i] field:0:required bufferSize
		// [i] field:0:required openTimeout
		// [o] object:0:required ehcacheOutputStream
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		//create the OutputStream
		OutputStream ehcacheOutputStream = createOutputStream(pipelineCursor);
		
		//output section
		pipelineCursor.last();
		
		//output stream
		pipelineCursor.insertAfter("ehcacheOutputStream", ehcacheOutputStream);
		
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
		// [i] field:0:required includeNoReads
		// [i] field:0:required includeNoWrites
		// [i] field:0:required includeReadsOnly
		// [i] field:0:required includeWritesOnly
		// [o] object:1:required keys
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		//get cache handle
		Ehcache cache = getCache(pipelineCursor);
		
		String excludeExpiredKeysStr = null;
		if (pipelineCursor.first("excludeExpiredKeys"))
		{
			excludeExpiredKeysStr = (String) pipelineCursor.getValue();
		}
		
		String includeNoReadsStr = null;
		if (pipelineCursor.first("includeNoReads"))
		{
			includeNoReadsStr = (String) pipelineCursor.getValue();
		}
		
		String includeNoWritesStr = null;
		if (pipelineCursor.first("includeNoWrites"))
		{
			includeNoWritesStr = (String) pipelineCursor.getValue();
		}
		
		String includeReadsOnlyStr = null;
		if (pipelineCursor.first("includeReadsOnly"))
		{
			includeReadsOnlyStr = (String) pipelineCursor.getValue();
		}
		
		String includeWritesOnlyStr = null;
		if (pipelineCursor.first("includeWritesOnly"))
		{
			includeWritesOnlyStr = (String) pipelineCursor.getValue();
		}
		
		boolean excludeExpiredKeys = "true".equalsIgnoreCase(excludeExpiredKeysStr);
		
		//if attribute not specified, defaults to true (= include in results)
		boolean includeNoReads = "true".equalsIgnoreCase(includeNoReadsStr);
		boolean includeNoWrites = "true".equalsIgnoreCase(includeNoWritesStr);
		boolean includeReadsOnly = "true".equalsIgnoreCase(includeReadsOnlyStr);
		boolean includeWritesOnly = "true".equalsIgnoreCase(includeWritesOnlyStr);
		
		List keys = EhcacheIOStreams.getStreamEntryKeys(
				cache, 
				excludeExpiredKeys, 
				includeNoReads, 
				includeNoWrites, 
				includeReadsOnly, 
				includeWritesOnly
		);
		
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
		// [i] object:0:required bytesOrInputStream
		// [i] field:0:optional append {"true","false"}
		// [i] field:0:optional compress {"true","false"}
		// [i] field:0:required bufferSize
		// [i] field:0:required openTimeout
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		InputStream inputStream = null;
		if (pipelineCursor.first("bytesOrInputStream"))
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
		
		if(null == inputStream)
			throw new ServiceException("inputStream provided is null...should not be.");
		
		//copy buffer size
		int copyBufferSize = DEFAULT_COPY_BUFFERSIZE;
		if (pipelineCursor.first("bufferSize"))
		{
			String copyBufferSizeStr = (String)pipelineCursor.getValue();
			try{
				if(null != copyBufferSizeStr && !"".equals(copyBufferSizeStr))
					copyBufferSize = Integer.parseInt(copyBufferSizeStr);
			} catch (NumberFormatException nfe){
				copyBufferSize = DEFAULT_COPY_BUFFERSIZE;
			}	
		}
		
		//create the OutputStream and copy to it directly
		OutputStream ehcacheOutputStream = null;
		try {
			ehcacheOutputStream = createOutputStream(pipelineCursor);
		
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
		
		//get cache handle
		Ehcache cache = getCache(pipelineCursor);
		
		Object cacheKey = null;
		if (pipelineCursor.first("key"))
		{
			//get the filename string object out of the pipeline
			cacheKey = (String) pipelineCursor.getValue();
		}
		
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
	static int DEFAULT_COPY_BUFFERSIZE = 128*1024; //default ehcache outputstream buffer size
	static boolean DEFAULT_GET_ALLOW_NULL_STREAM = true; // allow null stream on get when key not found
	
	public static OutputStream createOutputStream(IDataCursor pipelineCursor) throws ServiceException {
		Object cacheKey = null;
		String appendStr = null;
		String compressStr = null;
		
		//get cache handle
		Ehcache cache = getCache(pipelineCursor);
		
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
		
		boolean append = (null != appendStr && "true".equalsIgnoreCase(appendStr));
		boolean compress = (null != compressStr && "true".equalsIgnoreCase(compressStr));
	
		//copy buffer size
		Integer copyBufferSize = null;
		if (pipelineCursor.first("bufferSize"))
		{
			String copyBufferSizeStr = (String)pipelineCursor.getValue();
			try{
				if(null != copyBufferSizeStr && !"".equals(copyBufferSizeStr))
					copyBufferSize = Integer.parseInt(copyBufferSizeStr);
			} catch (NumberFormatException nfe){
				copyBufferSize = null;
			}	
		}
	
		//open timeout
		Long openTimeout = null;
		if (pipelineCursor.first("openTimeout"))
		{
			String openTimeoutStr = (String)pipelineCursor.getValue();
			try{
				if(null != openTimeoutStr && !"".equals(openTimeoutStr))
					openTimeout = Long.parseLong(openTimeoutStr);
			} catch (NumberFormatException nfe){
				openTimeout = null;
			}	
		}
		
		return createOutputStream(cache, cacheKey, append, compress, copyBufferSize, openTimeout);
	}
	
	public static OutputStream createOutputStream(Ehcache cache, Object cacheKey, boolean append, boolean compress, Integer bufferSize, Long openTimeout) throws ServiceException {
		validateParamsCacheKey(cacheKey);
		
		OutputStream ehcacheOutputStream = null;
		try {
			if(null == bufferSize && null == openTimeout)
				ehcacheOutputStream = EhcacheIOStreams.getOutputStream(cache, cacheKey, !append);
			else if (null != bufferSize && null == openTimeout)
				ehcacheOutputStream = EhcacheIOStreams.getOutputStream(cache, cacheKey, !append, bufferSize);
			else if (null == bufferSize && null != openTimeout)
				ehcacheOutputStream = EhcacheIOStreams.getOutputStream(cache, cacheKey, !append, DEFAULT_COPY_BUFFERSIZE, openTimeout);
			else if (null != bufferSize && null != openTimeout)
				ehcacheOutputStream = EhcacheIOStreams.getOutputStream(cache, cacheKey, !append, bufferSize, openTimeout);
				
			if(compress)
				ehcacheOutputStream = new GZIPOutputStream(ehcacheOutputStream);
		} catch (IOException e) {
			closeStream(ehcacheOutputStream);
			ehcacheOutputStream = null;
			throw new ServiceException(e);
		}
		return ehcacheOutputStream;
	}
	
	public static InputStream createInputStream(IDataCursor pipelineCursor) throws ServiceException {
		String decompressStr = null;
		Object cacheKey = null;
		
		//get cache handle
		Ehcache cache = getCache(pipelineCursor);
		
		if (pipelineCursor.first("key"))
		{
			//get the filename string object out of the pipeline
			cacheKey = (String) pipelineCursor.getValue();
		}
		
		if (pipelineCursor.first("decompress"))
		{
			//get the filename string object out of the pipeline
			decompressStr = (String) pipelineCursor.getValue();
		}
	
		//copy buffer size
		Integer copyBufferSize = null;
		if (pipelineCursor.first("bufferSize"))
		{
			String copyBufferSizeStr = (String)pipelineCursor.getValue();
			try{
				if(null != copyBufferSizeStr && !"".equals(copyBufferSizeStr))
					copyBufferSize = Integer.parseInt(copyBufferSizeStr);
			} catch (NumberFormatException nfe){
				copyBufferSize = null;
			}	
		}
	
		//open timeout
		Long openTimeout = null;
		if (pipelineCursor.first("openTimeout"))
		{
			String openTimeoutStr = (String)pipelineCursor.getValue();
			try{
				if(null != openTimeoutStr && !"".equals(openTimeoutStr))
					openTimeout = Long.parseLong(openTimeoutStr);
			} catch (NumberFormatException nfe){
				openTimeout = null;
			}	
		}
	
		//parse decompress value
		boolean decompress = (null != decompressStr && "true".equalsIgnoreCase(decompressStr));
		
		//force returning null on key not found
		boolean allowNullStream = DEFAULT_GET_ALLOW_NULL_STREAM;
		
		return createInputStream(cache, cacheKey, allowNullStream, decompress, copyBufferSize, openTimeout);
	}
	
	public static InputStream createInputStream(Ehcache cache, Object cacheKey, boolean allowNullStream, boolean decompress, Integer bufferSize, Long openTimeout) throws ServiceException {
		validateParamsCacheKey(cacheKey);
		
		InputStream ehcacheInputStream = null;
		try {
			
			if(null == bufferSize && null == openTimeout)
				ehcacheInputStream = EhcacheIOStreams.getInputStream(cache, cacheKey, allowNullStream);
			else if (null != bufferSize && null == openTimeout)
				ehcacheInputStream = EhcacheIOStreams.getInputStream(cache, cacheKey, allowNullStream, bufferSize);
			else if (null == bufferSize && null != openTimeout)
				ehcacheInputStream = EhcacheIOStreams.getInputStream(cache, cacheKey, allowNullStream, DEFAULT_COPY_BUFFERSIZE, openTimeout);
			else if (null != bufferSize && null != openTimeout)
				ehcacheInputStream = EhcacheIOStreams.getInputStream(cache, cacheKey, allowNullStream, bufferSize, openTimeout);
			
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
		return ehcacheInputStream;
	}
	
	public static void validateParamsCacheKey(Object cacheKey) throws ServiceException {
		if (cacheKey == null)
			throw new ServiceException("cacheKey may not be null");
		else
			return;
	}
	
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
	
	public static Ehcache getCache(IDataCursor pipelineCursor) throws ServiceException {
		String cacheManagerName = null;
		String cacheName = null;
		Ehcache cache = null;
		
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
		
		//get the cache
		cache = getCache(cacheManagerName, cacheName);
		
		return cache;
	}
	
	public static Ehcache getCache(String cacheManagerName, String cacheName) throws ServiceException {
		Ehcache cache = null;
		validateParams(cacheManagerName, cacheName);
		if (CacheManagerConfig.cacheManagerAlive(cacheManagerName)) {
			CacheManager cacheManager = getCacheManager(cacheManagerName);
			cache = cacheManager.getEhcache(cacheName);
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

