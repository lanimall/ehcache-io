# ehcache-io: Input/Output Streams for Ehcache

## Description
This extension creates a streaming capability (java.io) for Ehcache so clients can easily push/pull to/from ehcache with standard InputStream/OutputStream objects.
 * Stream objects/files into Ehcache (and/or into terracotta if your cache is distributed)
 * Stream objects/files from Ehcache (and/or from terracotta if your cache is distributed) back to your JVM for further processing, or further streaming to another Outputstream.

The general value of using such construct is to be able to process and store large objects little by little without having to load them all in your program's heap memory.
So this could be particularly useful for dealing with large data items (big xmls, binary files, images) that you'd want to store temporarily out of the heap while doing or waiting for something else...
and especially useful if your Ehcache caches leverage offheap memory and Terracotta for clustering.

Essentially, this implementation creates 2 new Ehcache-specific stream classes which are compliant with java.io InputStream and OutputStream contracts:
 * EhcacheOutputStream: writes to underlying Ehcache
 * EhcacheInputStream: reads from underlying Ehcache

The 2 main rationale for using these streaming classes are:
 * Plug-ability: You can plug these Ehcache stream into the extensive library of java.io streams (eg. File streams, socket streams, pipe streans, Checksum streams, Gzip streams, etc…)
 * JVM Memory control: When dealing with large files, you can overload local JVM memory very easily if you're loading them in heap. 
 But when you're using a "streaming" construct, only the bytes (or chunks of bytes) passing through are loaded in JVM Heap. 
 So for example, you could stream a 1GB file in Ehcache even if the client heap is 512MB.
 * Ability to append to an existing cache key without the need to bring the current cached object back to heap. 
 Essentially, set override=false on the Ehcache OutputStream and you'll have the ability to append new data to the value identified by the key

## Building

```java
mvn clean package
```

If you want to run the EE Tests (eg. Offheap or Terracotta clustering), then use the ehcache-ee profile.

```java
mvn clean package -P ehcache-ee
```

## Usage / Features:

Both Input/Output Streams can be acquired from the factory "EhcacheIOStreams" with 2 main static calls, 
always providing AT LEAST the underlying Ehcache "cache" and "cachekey".
Then, couple of optional behaviors can be used too. Here are simple details for each of the paramaters:

 * EhcacheIOStreams.getInputStream
   * Cache cache (REQUIRED: the underlying cache that this stream will pull from)
   * Object cacheKey (REQUIRED: the underlying cache key that this stream will pull from)
   * boolean allowNullStream (OPTIONAL: Ability to specify if you want to get a NULL stream if the underlying cacheKey does not exist -- See "Default Settings" for default value)
   * int bufferSize (OPTIONAL: internal read buffer -- See "Default Settings" for default value)
   * long openTimeout (OPTIONAL: When opening a stream, max time to wait before exception occurs -- See "Default Settings" for default value)
 
 * EhcacheIOStreams.getOutputStream
   * Cache cache (REQUIRED: the underlying cache that this stream will pull from)
   * Object cacheKey (REQUIRED: the underlying cache key that this stream will pull from)
   * boolean override (OPTIONAL: If true, any new data will override existing data for that same cacheKey. If false, data will be appended to that same cacheKey -- See "Default Settings" for default value)
   * int bufferSize (OPTIONAL: Internal write buffer - This will be the block size in ehcache storage -- See "Default Settings" for default value)
   * long openTimeout (OPTIONAL: When opening a stream, max time to wait before exception occurs -- See "Default Settings" for default value)

 * EhcacheIOStreams.checkStreamEntryExist(Cache cache, Object cacheKey)
   * Check if a stream entry exists
   
 * EhcacheIOStreams.removeStreamEntry(Cache cache, Object cacheKey)
   * Remove a stream entry
  
## Default Settings

If you want to specify some custom global defaults for your application, I also added some system properties for that purpose:
 * ehcache.extension.io.inputstream.buffersize (global default size for the read buffer. If not specified, default is 512KB)
 * ehcache.extension.io.outputstream.buffersize (global default size for the write buffer. If not specified, default is 1MB)
 * ehcache.extension.io.outputstream.override (global default for output stream override value. If not specified, default is TRUE)
 * ehcache.extension.io.streams.opentimeout (global default that specifies the timeout when trying to open a stream. If not specified, default is 10s)
 * ehcache.extension.io.streams.allownull (global default that specifies if you are ok returning null streams when a stream entry is not in cache. If not specified, default is FALSE)

## Code Samples:

A small test app is available at [./ehcache-io-sampleapp](./ehcache-io-sampleapp) so check it out for working code.

But essentially, here are 2 main code snippets for both Input and Output stream constructs:

1 - Copy local file into cache - FileInputStream to EhcacheOutputStream sample:
Note: I'm also using the CheckedInputStream here mostly to demonstrate plug-ability, but also for my own junit tests, making sure consistency before and after streaming.

```java
       ...
       try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(EhcacheIOStreams.getOutputStream(cache, cache_key),new CRC32());
       )
       {
            Int in;
            byte[] buffer = new byte[bufferSize];
            while ((n = is.read(buffer)) > -1) {
                os.write(buffer, 0, n);
            }

            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
       }
       ...
```

2 - Copy a file in cache back to a local file - EhcacheInputStream to FileOutputStream
Note: I'm also using the CheckedInputStream here mostly to demonstrate plug-ability, but also for my own junit tests, making sure consistency before and after streaming.

```java
       try (
                CheckedInputStream is = new CheckedInputStream(EhcacheIOStreams.getInputStream(cache, cache_key),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(OUT_FILE_PATH)), new CRC32());
       )
       {
            int n;
            byte[] buffer = new byte[bufferSize];
            while ((n = is.read(buffer)) > -1) {
                os.write(buffer, 0, n);
            }

            Assert.assertEquals(is.getChecksum().getValue(), os.getChecksum().getValue());
       }
```