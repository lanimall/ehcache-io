package org.ehcache.extensions.io.impl.utils;

import java.util.Properties;

/**
 * Created by fabien.sanglier on 9/14/18.
 */
public class PropertyUtils {
    public static final String PROP_INPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.inputstream.buffersize";
    public static final String PROP_INPUTSTREAM_OPEN_TIMEOUTS = "ehcache.extension.io.inputstream.opentimeout";
    public static final String PROP_INPUTSTREAM_ALLOW_NULLSTREAM = "ehcache.extension.io.inputstream.allownull";
    public static final String PROP_OUTPUTSTREAM_BUFFERSIZE = "ehcache.extension.io.outputstream.buffersize";
    public static final String PROP_OUTPUTSTREAM_OVERRIDE = "ehcache.extension.io.outputstream.override";
    public static final String PROP_OUTPUTSTREAM_OPEN_TIMEOUTS = "ehcache.extension.io.outputstream.opentimeout";

    public static final String PROP_CONCURRENCY_MODE = "ehcache.extension.io.concurrency.mode";

    public static final String PROP_OUTPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_BASE_MILLIS = "ehcache.extension.io.outputstream.concurrency.cas.backoff.exponential.base";
    public static final String PROP_OUTPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_CAP_MILLIS = "ehcache.extension.io.outputstream.concurrency.cas.backoff.exponential.cap";
    public static final String PROP_INPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_BASE_MILLIS = "ehcache.extension.io.inputstream.concurrency.cas.backoff.exponential.base";
    public static final String PROP_INPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_CAP_MILLIS = "ehcache.extension.io.inputstream.concurrency.cas.backoff.exponential.cap";

    public static final int DEFAULT_OUTPUTSTREAM_BUFFER_SIZE = 256 * 1024; // 256kb
    public static final boolean DEFAULT_OUTPUTSTREAM_OVERRIDE = true;
    public static final int DEFAULT_INPUTSTREAM_BUFFER_SIZE = 512 * 1024; // 512kb
    public static final long DEFAULT_OUTPUTSTREAM_OPEN_TIMEOUT = 10000L;
    public static final long DEFAULT_INPUTSTREAM_OPEN_TIMEOUT = 2000L;
    public static final boolean DEFAULT_INPUTSTREAM_ALLOW_NULLSTREAM = false;

    public static final long DEFAULT_OUTPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_BASE_MILLIS = 2;
    public static final long DEFAULT_OUTPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_CAP_MILLIS = 50;
    public static final long DEFAULT_INPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_BASE_MILLIS = 1;
    public static final long DEFAULT_INPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_CAP_MILLIS = 20;

    //creating an exponential wait object to be use for busy wait cas loops
    public static final WaitStrategy defaultWritesCasBackoffWaitStrategy = new ExponentialWait(
            PropertyUtils.getOutputStreamCasLoopExponentialBackoffBase(),
            PropertyUtils.getOutputStreamCasLoopExponentialBackoffCap()
    );

    //creating an exponential wait object to be use for busy wait cas loops
    public static final WaitStrategy defaultReadsCasBackoffWaitStrategy = new ExponentialWait(
            PropertyUtils.getInputStreamCasLoopExponentialBackoffBase(),
            PropertyUtils.getInputStreamCasLoopExponentialBackoffCap()
    );

    public static final ConcurrencyMode DEFAULT_CONCURRENCY_MODE = ConcurrencyMode.READ_COMMITTED_CASLOCKS;

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
    public static final long getOutputStreamCasLoopExponentialBackoffBase(){
        return getPropertyAsLong(PROP_OUTPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_BASE_MILLIS, DEFAULT_OUTPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_BASE_MILLIS);
    }
    public static final long getOutputStreamCasLoopExponentialBackoffCap(){
        return getPropertyAsLong(PROP_OUTPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_CAP_MILLIS, DEFAULT_OUTPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_CAP_MILLIS);
    }
    public static final long getInputStreamCasLoopExponentialBackoffBase(){
        return getPropertyAsLong(PROP_INPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_BASE_MILLIS, DEFAULT_INPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_BASE_MILLIS);
    }
    public static final long getInputStreamCasLoopExponentialBackoffCap(){
        return getPropertyAsLong(PROP_INPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_CAP_MILLIS, DEFAULT_INPUTSTREAM_CONCURRENCY_CAS_LOOP_BACKOFF_EXP_CAP_MILLIS);
    }

    public static String getPropertyAsString(final Properties properties, final String key, final String defaultVal) {
        if(null == properties)
            throw new IllegalStateException("Properties may not be null.");

        return properties.getProperty(key, defaultVal);
    }

    public static String getPropertyAsString(final String key, final String defaultVal) {
        return getPropertyAsString(System.getProperties(), key, defaultVal);
    }

    public static long getPropertyAsLong(final String key, final long defaultVal) {
        return getPropertyAsLong(System.getProperties(), key, defaultVal);
    }

    public static long getPropertyAsLong(final Properties properties, final String key, final long defaultVal) {
        String valStr = getPropertyAsString(properties, key, new Long(defaultVal).toString());
        long val;
        try {
            val = Long.parseLong(valStr);
        } catch (NumberFormatException nfe) {
            val = defaultVal;
        }
        return val;
    }

    public static int getPropertyAsInt(final String key, final int defaultVal) {
        return getPropertyAsInt(System.getProperties(), key, defaultVal);
    }

    public static int getPropertyAsInt(final Properties properties, final String key, final int defaultVal) {
        String valStr = getPropertyAsString(properties, key, new Integer(defaultVal).toString());
        int val;
        try {
            val = Integer.parseInt(valStr);
        } catch (NumberFormatException nfe) {
            val = defaultVal;
        }
        return val;
    }

    public static boolean getPropertyAsBoolean(final String key, final boolean defaultVal) {
        return getPropertyAsBoolean(System.getProperties(), key, defaultVal);
    }

    public static boolean getPropertyAsBoolean(final Properties properties, final String key, final boolean defaultVal) {
        String valStr = getPropertyAsString(properties, key, new Boolean(defaultVal).toString());
        return Boolean.parseBoolean(valStr);
    }

    public enum ConcurrencyMode {
        READ_COMMITTED_WITHLOCKS("read_committed_explicitlocking"),
        READ_COMMITTED_CASLOCKS("read_committed"),
        WRITE_PRIORITY("write_priority");

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
                else if (WRITE_PRIORITY.propValue.equalsIgnoreCase(concurrencyModeStr))
                    return WRITE_PRIORITY;
                else if (READ_COMMITTED_CASLOCKS.propValue.equalsIgnoreCase(concurrencyModeStr))
                    return READ_COMMITTED_CASLOCKS;
                else
                    throw new IllegalArgumentException("ConcurrencyMode [" + ((null != concurrencyModeStr) ? concurrencyModeStr : "null") + "] is not valid");
            } else {
                return DEFAULT_CONCURRENCY_MODE;
            }
        }

        @Override
        public String toString() {
            return "ConcurrencyMode{" +
                    "propValue='" + propValue + '\'' +
                    '}';
        }
    }
}
