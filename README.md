# ehcache-streaming

## Description
This extension creates a streaming capability for Ehcache, which allows you to:
 - Easily push new files into Ehcache (and/or into terracotta if your cache is distributed) — See sample usage below
 - Easily pull files from Ehcache (and/or from terracotta if your cache is distributed) back to your JVM for further processing, or further streaming to something else... — See sample usage below

This could be particularly useful for dealing with large data items (big xmls, binary files, images) via standard Ehcache API in local heap / offheap memory (and potentially in Terracotta as well)

Essentially, this implementation creates 2 new Ehcache-specific stream classes which are compliant with java.io InputStream and OutputStream contracts:
 - EhcacheOutputStream: writes to underlying Ehcache
 - EhcacheInputStream: reads from underlying Ehcache

The 2 main rationale for using these streaming classes are:
 - Plug-ability: You can plug these Ehcache stream into the extensive library of java.io streams (eg. File streams, socket streams, pipe streans, Checksum streams etc…)
 - JVM Memory control: When dealing with large files, you can overload local JVM memory very easily if you're loading them in heap (which standard Ehcache library would do). But when you're using a "streaming" construct, only the bytes (or chunks of bytes) passing through are loaded in JVM Heap. So for example, you could stream a 1GB file in Ehcache even if the client heap is 512MB.

## Simple usage:

1 - Copy local file into cache - FileInputStream to EhcacheOutputStream sample:
Note: I'm also using the CheckedInputStream here mostly to demonstrate plug-ability, but also for my own junit tests, making sure consistency before and after streaming.

```java
       ...
       try (
                CheckedInputStream is = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(IN_FILE_PATH),inBufferSize),new CRC32());
                CheckedOutputStream os = new CheckedOutputStream(new EhcacheOutputStream(cache, cache_key),new CRC32());
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
                CheckedInputStream is = new CheckedInputStream(new EhcacheInputStream(cache, cache_key),new CRC32());
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