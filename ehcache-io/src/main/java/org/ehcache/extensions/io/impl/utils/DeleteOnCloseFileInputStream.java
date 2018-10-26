package org.ehcache.extensions.io.impl.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by fabien.sanglier on 10/25/18.
 */
public class DeleteOnCloseFileInputStream extends FileInputStream {
    private File fileReference;

    public DeleteOnCloseFileInputStream(String name) throws FileNotFoundException {
        this(null != name? new File(name):null);
    }

    public DeleteOnCloseFileInputStream(File file) throws FileNotFoundException {
        super(file);
        this.fileReference = file;
    }

    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if(fileReference != null) {
                fileReference.delete();
                fileReference = null;
            }
        }
    }
}