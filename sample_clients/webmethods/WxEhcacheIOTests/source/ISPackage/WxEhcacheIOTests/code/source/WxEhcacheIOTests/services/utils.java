package WxEhcacheIOTests.services;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import java.io.*;
// --- <<IS-END-IMPORTS>> ---

public final class utils

{
	// ---( internal utility methods )---

	final static utils _instance = new utils();

	static utils _newInstance() { return new utils(); }

	static utils _cast(Object o) { return (utils)o; }

	// ---( server methods )---




	public static final void readFullStream (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(readFullStream)>> ---
		// @sigtype java 3.5
		// [i] object:0:required inputStream
		// [o] field:0:required length
		// [o] field:0:required success
		IDataCursor cursor = pipeline.getCursor();
		
		boolean success = false;
		int readLength = 0;
		try
		{
			if (cursor.first("inputStream"))
			{
				InputStream inputStream = (InputStream) cursor.getValue();
				String charsetName = IDataUtil.getString(cursor, "charsetName");
		
				// Create a BufferedReader from the input stream
				BufferedReader reader = null;
				if (charsetName != null)
				{
					reader = new BufferedReader(new InputStreamReader(inputStream, charsetName));
				}
				else
				{
					reader = new BufferedReader(new InputStreamReader(inputStream));
				}
		
				readLength = 0;
				while (reader.read() > -1){
					readLength++;
				}
				
				success = true;
			}
		}
		catch (UnsupportedEncodingException use)
		{
			throw new ServiceException(use);
		} catch (IOException e) {
			throw new ServiceException(e);
		}
		finally
		{	    
			//output section
			cursor.last();
					
			cursor.insertAfter("success", new Boolean(success).toString());
			cursor.insertAfter("length", new Integer(readLength).toString());
			
			cursor.destroy();
		}
		// --- <<IS-END>> ---

                
	}
}

