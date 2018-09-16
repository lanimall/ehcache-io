package org.ehcache.extensions.io.impl.utils;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public class PropertyUtils {
    public static final String PROP_CONCURRENCY_MODE = "ehcache.extension.io.concurrency.mode";
    public static final String PROP_INPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.inputstream.buffersize";
    public static final String PROP_INPUTSTREAM_OPEN_TIMEOUTS = "ehcache.extension.io.inputstream.opentimeout";
    public static final String PROP_INPUTSTREAM_ALLOW_NULLSTREAM = "ehcache.extension.io.inputstream.allownull";
    public static final String PROP_OUTPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.outputstream.buffersize";
    public static final String PROP_OUTPUTSTREAM_OVERRIDE = "ehcache.extension.io.outputstream.override";
    public static final String PROP_OUTPUTSTREAM_OPEN_TIMEOUTS = "ehcache.extension.io.outputstream.opentimeout";

    public static final int DEFAULT_OUTPUTSTREAM_BUFFER_SIZE = 128 * 1024; // 128kb
    public static final boolean DEFAULT_OUTPUTSTREAM_OVERRIDE = true;
    public static final int DEFAULT_INPUTSTREAM_BUFFER_SIZE = 512 * 1024; // 512kb
    public static final long DEFAULT_OUTPUTSTREAM_OPEN_TIMEOUT = 10000L;
    public static final long DEFAULT_INPUTSTREAM_OPEN_TIMEOUT = 2000L;
    public static final boolean DEFAULT_INPUTSTREAM_ALLOW_NULLSTREAM = false;
    public static final long DEFAULT_BUSYWAIT_RETRY_LOOP_SLEEP_MILLIS = 50;
    public static final long DEFAULT_CONSISTENCY_WAIT_TIMEOUT = 100;

    public static final ConcurrencyMode DEFAULT_CONCURRENCY_MODE = ConcurrencyMode.WRITE_PRIORITY_NOLOCK;

    public static final Integer getInputStreamBufferSize(){
        return getPropertyAsInt(PROP_INPUTSTREAM_BUFFERSIZE, DEFAULT_INPUTSTREAM_BUFFER_SIZE);
    }
    public static final Integer getOutputStreamBufferSize(){
        return getPropertyAsInt(PROP_OUTPUTSTREAM_BUFFERSIZE, DEFAULT_OUTPUTSTREAM_BUFFER_SIZE);
    }
    public static final Long getInputStreamOpenTimeout(){
        return getPropertyAsLong(PROP_INPUTSTREAM_OPEN_TIMEOUTS, DEFAULT_INPUTSTREAM_OPEN_TIMEOUT);
    }
    public static final Long getOutputStreamOpenTimeout(){
        return getPropertyAsLong(PROP_OUTPUTSTREAM_OPEN_TIMEOUTS, DEFAULT_OUTPUTSTREAM_OPEN_TIMEOUT);
    }
    public static final Boolean getInputStreamAllowNulls(){
        return getPropertyAsBoolean(PROP_INPUTSTREAM_ALLOW_NULLSTREAM, DEFAULT_INPUTSTREAM_ALLOW_NULLSTREAM);
    }
    public static final Boolean getOutputStreamDefaultOverride(){
        return getPropertyAsBoolean(PROP_OUTPUTSTREAM_OVERRIDE, DEFAULT_OUTPUTSTREAM_OVERRIDE);
    }
    public static final ConcurrencyMode getEhcacheIOStreamsConcurrencyMode(){
        return ConcurrencyMode.valueOfIgnoreCase(getPropertyAsString(PROP_CONCURRENCY_MODE, DEFAULT_CONCURRENCY_MODE.getPropValue()));
    }

    private static String getPropertyAsString(String key, String defaultVal) {
        return System.getProperty(key, defaultVal);
    }

    private static long getPropertyAsLong(String key, long defaultVal) {
        String valStr = System.getProperty(key, new Long(defaultVal).toString());
        long val;
        try {
            val = Long.parseLong(valStr);
        } catch (NumberFormatException nfe) {
            val = defaultVal;
        }
        return val;
    }

    private static int getPropertyAsInt(String key, int defaultVal) {
        String valStr = System.getProperty(key, new Integer(defaultVal).toString());
        int val;
        try {
            val = Integer.parseInt(valStr);
        } catch (NumberFormatException nfe) {
            val = defaultVal;
        }
        return val;
    }

    private static boolean getPropertyAsBoolean(String key, boolean defaultVal) {
        String valStr = System.getProperty(key, new Boolean(defaultVal).toString());
        return Boolean.parseBoolean(valStr);
    }

    public enum ConcurrencyMode {
        READ_COMMITTED_WITHLOCKS("read_committed"),
        WRITE_PRIORITY_NOLOCK("write_priority");

        private final String propValue;
        ConcurrencyMode(String propValue) {
            this.propValue = propValue;
        }

        public String getPropValue() {
            return propValue;
        }

        public static ConcurrencyMode valueOfIgnoreCase(String concurrencyModeStr){
            if(null != concurrencyModeStr && !"".equals(concurrencyModeStr)) {
                if (READ_COMMITTED_WITHLOCKS.propValue.equalsIgnoreCase(concurrencyModeStr))
                    return READ_COMMITTED_WITHLOCKS;
                else if (WRITE_PRIORITY_NOLOCK.propValue.equalsIgnoreCase(concurrencyModeStr))
                    return WRITE_PRIORITY_NOLOCK;
                else
                    throw new IllegalArgumentException("ConcurrencyMode [" + ((null != concurrencyModeStr) ? concurrencyModeStr : "null") + "] is not valid");
            } else {
                return DEFAULT_CONCURRENCY_MODE;
            }
        }
    }
}
