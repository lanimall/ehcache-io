package org.ehcache.extensions.io.impl.readers;

import org.ehcache.extensions.io.EhcacheStreamException;
import org.ehcache.extensions.io.impl.utils.DeleteOnCloseFileInputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Created by fabien.sanglier on 10/25/18.
 */
public class EhcacheFileAdapterInputStream extends InputStream {
    private final EhcacheInputStream ehcacheInputStream;
    private DeleteOnCloseFileInputStream deleteOnCloseFileInputStream;

    private static final String FILE_PREFIX = "EhcacheIOStream";
    private final Path fileDirPath;

    private final boolean enablediskOffload;
    private final Object onDiskPadLock = new Object();
    private volatile boolean onDisk = false;

    private final int copyBufferSize = 512 * 1024; //copy buffer size

    public EhcacheFileAdapterInputStream(EhcacheInputStream ehcacheInputStream, String fileDirPath, long sizeInBytesThreshold) throws EhcacheStreamException {
        if(null == ehcacheInputStream)
            throw new IllegalStateException("ehcacheInputStream may not be null");

        this.ehcacheInputStream = ehcacheInputStream;
        this.fileDirPath = FileSystems.getDefault().getPath(fileDirPath);
        this.enablediskOffload = ehcacheInputStream.available() > sizeInBytesThreshold;
    }

    private void pushEntryToDisk() throws IOException {
        if(!onDisk){
            synchronized (onDiskPadLock){
                if(!onDisk) {
                    //construct the full path
                    String ehcacheTempFile = String.format("%s-%s-%d.tmp", FILE_PREFIX, UUID.randomUUID().toString(), ehcacheInputStream.getPublicCacheKey().hashCode());
                    Path ehcacheEntryFile = FileSystems.getDefault().getPath(fileDirPath.toString(), ehcacheTempFile);

                    //copy cache entry to temp file
                    EhcacheInputStream is = null;
                    FileOutputStream os = null;
                    try {
                        is = ehcacheInputStream;
                        os = new FileOutputStream(ehcacheEntryFile.toString(),false);

                        int n;
                        byte[] buffer = new byte[copyBufferSize];
                        while ((n = is.read(buffer)) > -1) {
                            os.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
                        }
                    } finally {
                        if(null != is)
                            is.close();

                        if(null != os)
                            os.close();
                    }

                    //initialize the file input stream here now the file was copied
                    deleteOnCloseFileInputStream = new DeleteOnCloseFileInputStream(ehcacheEntryFile.toString());

                    onDisk = true;
                }
            }
        }
    }

    @Override
    public int read() throws IOException {
        if(enablediskOffload) {
            pushEntryToDisk();
            return deleteOnCloseFileInputStream.read();
        } else {
            return ehcacheInputStream.read();
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        if(enablediskOffload) {pushEntryToDisk();
            return deleteOnCloseFileInputStream.read(b);
        } else {
            return ehcacheInputStream.read(b);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if(enablediskOffload) {pushEntryToDisk();
            return deleteOnCloseFileInputStream.read(b, off, len);
        } else {
            return ehcacheInputStream.read(b, off, len);
        }
    }

    @Override
    public long skip(long n) throws IOException {
        if(enablediskOffload) {
            pushEntryToDisk();
            return deleteOnCloseFileInputStream.skip(n);
        } else {
            return ehcacheInputStream.skip(n);
        }
    }

    @Override
    public int available() throws IOException {
        if(enablediskOffload) {
            pushEntryToDisk();
            return deleteOnCloseFileInputStream.available();
        } else {
            return ehcacheInputStream.available();
        }
    }

    @Override
    public void close() throws IOException {
        if(enablediskOffload) {
            pushEntryToDisk();
            deleteOnCloseFileInputStream.close();
        } else {
            ehcacheInputStream.close();
        }
    }

    @Override
    public synchronized void mark(int readlimit) {
        ;; //do nothing
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
