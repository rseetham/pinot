/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.plugin.inputformat.protobuf.codegen.MessageCodeGen;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamMessageDecoder} for Protocol Buffer messages that uses runtime code generation and Janino compilation
 * to produce an efficient, reflection-free decode path.
 *
 * <p>The generated {@link Method} is deterministic given {@code (jarFile, protoClassName, fieldsToRead)} and is
 * expensive to produce (remote JAR fetch + Janino compilation). A JVM-level cache keeps the last-known-good
 * {@link Method} per unique input combination and refreshes it proactively in the background, so segment creation
 * never blocks on a cold compile after the first initialization.
 *
 * <p>Cache behaviour:
 * <ul>
 *   <li>On the first {@link #init} for a given key the decode method is built synchronously.</li>
 *   <li>All subsequent {@link #init} calls with the same key return the cached value immediately.</li>
 *   <li>A single daemon {@link ScheduledExecutorService} refreshes each known key periodically. On refresh
 *       failure the stale value is retained and a warning is logged.</li>
 *   <li>The backing {@link URLClassLoader} of a replaced entry is closed to prevent file-handle leaks.</li>
 * </ul>
 *
 * <p>Thread-safety: all static state is either a {@link ConcurrentHashMap} or guarded by
 * {@link #_schedulerLock}.
 */
public class ProtoBufCodeGenMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufCodeGenMessageDecoder.class);

  public static final String PROTOBUF_JAR_FILE_PATH = "jarFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";
  /**
   * Decoder prop key for the background cache refresh interval in minutes.
   * Set via table config: {@code stream.<type>.decoder.prop.codegenCacheRefreshIntervalMinutes}.
   * Because the cache is JVM-level static, the interval of the first decoder to register a given key wins.
   */
  public static final String CODEGEN_CACHE_REFRESH_INTERVAL_MINUTES = "codegenCacheRefreshIntervalMinutes";

  private static final int DEFAULT_CACHE_REFRESH_INTERVAL_MINUTES = 60;

  /**
   * Holds the original inputs alongside the compiled {@link Method} so that the background refresh
   * task can re-run the build pipeline without parsing the cache key string.
   */
  static final class CacheEntry {
    final String _jarPath;
    final String _protoClassName;
    final Set<String> _fieldsToRead;
    final Method _method;

    CacheEntry(String jarPath, String protoClassName, Set<String> fieldsToRead, Method method) {
      _jarPath = jarPath;
      _protoClassName = protoClassName;
      _fieldsToRead = fieldsToRead;
      _method = method;
    }
  }

  // Last-known-good CacheEntry per cache key. Written by the background refresh thread and
  // by init() on first population. Read by all subsequent init() calls without blocking.
  private static final ConcurrentHashMap<String, CacheEntry> METHOD_CACHE = new ConcurrentHashMap<>();

  // Tracks the scheduled refresh future per key so each key is refreshed at most once.
  private static final ConcurrentHashMap<String, ScheduledFuture<?>> REFRESH_TASKS = new ConcurrentHashMap<>();

  // Single shared daemon scheduler, created lazily on first use.
  private static volatile ScheduledExecutorService _refreshScheduler;
  private static final Object SCHEDULER_LOCK = new Object();

  private Method _decodeMethod;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(PROTOBUF_JAR_FILE_PATH),
        "Protocol Buffer schema jar file must be provided");
    Preconditions.checkState(props.containsKey(PROTO_CLASS_NAME),
        "Protocol Buffer Message class name must be provided");
    String protoClassName = props.get(PROTO_CLASS_NAME);
    String jarPath = props.get(PROTOBUF_JAR_FILE_PATH);
    int refreshIntervalMinutes = Integer.parseInt(
        props.getOrDefault(CODEGEN_CACHE_REFRESH_INTERVAL_MINUTES,
            String.valueOf(DEFAULT_CACHE_REFRESH_INTERVAL_MINUTES)));

    String cacheKey = buildCacheKey(jarPath, protoClassName, fieldsToRead);

    // Fast path: return the cached Method immediately without any I/O or compilation.
    CacheEntry cached = METHOD_CACHE.get(cacheKey);
    if (cached != null) {
      _decodeMethod = cached._method;
      scheduleRefreshIfAbsent(cacheKey, refreshIntervalMinutes);
      return;
    }

    // Cold path: first init for this key — build synchronously.
    Method method = buildDecodeMethod(jarPath, protoClassName, fieldsToRead);
    METHOD_CACHE.put(cacheKey, new CacheEntry(jarPath, protoClassName, fieldsToRead, method));
    _decodeMethod = method;
    scheduleRefreshIfAbsent(cacheKey, refreshIntervalMinutes);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      destination = (GenericRow) _decodeMethod.invoke(null, payload, destination);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while decoding protobuf message", e);
    }
    return destination;
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    if (offset != 0 || payload.length > length) {
      payload = Arrays.copyOfRange(payload, offset, offset + length);
    }
    return decode(payload, destination);
  }

  // ---------------------------------------------------------------------------
  // Cache helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a stable cache key from the three inputs that fully determine the generated code.
   * {@code fieldsToRead} is sorted so that Set iteration-order differences do not produce distinct keys.
   */
  static String buildCacheKey(String jarPath, String protoClassName, Set<String> fieldsToRead) {
    String fields = String.join(",", new TreeSet<>(fieldsToRead));
    return URI.create(jarPath).normalize() + "|" + protoClassName + "|" + fields;
  }

  /**
   * Schedules a periodic background refresh for {@code cacheKey} if one is not already running.
   * The call is idempotent: repeated invocations with the same key are no-ops.
   */
  private static void scheduleRefreshIfAbsent(String cacheKey, int intervalMinutes) {
    REFRESH_TASKS.computeIfAbsent(cacheKey, key -> {
      ScheduledExecutorService scheduler = getOrCreateScheduler();
      return scheduler.scheduleWithFixedDelay(
          () -> refreshKey(key), intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
    });
  }

  /**
   * Background refresh task. Re-builds the decode {@link Method} for {@code cacheKey} and atomically
   * replaces the cached entry. The old {@link URLClassLoader} is closed to prevent file-handle leaks.
   * On failure the existing cached value is left unchanged and a warning is logged.
   */
  private static void refreshKey(String cacheKey) {
    CacheEntry existing = METHOD_CACHE.get(cacheKey);
    if (existing == null) {
      return;
    }
    try {
      Method fresh = buildDecodeMethod(existing._jarPath, existing._protoClassName, existing._fieldsToRead);
      CacheEntry old = METHOD_CACHE.put(cacheKey,
          new CacheEntry(existing._jarPath, existing._protoClassName, existing._fieldsToRead, fresh));
      closeClassLoader(old);
    } catch (Exception e) {
      LOGGER.warn("Background refresh of protobuf codegen cache failed for key: {}. "
          + "Stale compiled method will continue to be used until the next successful refresh.", cacheKey, e);
      // TODO: emit a PinotMeter metric here once StreamMessageDecoder.init() exposes a metrics registry.
    }
  }

  /** Lazily creates the shared daemon scheduler using double-checked locking. */
  private static ScheduledExecutorService getOrCreateScheduler() {
    if (_refreshScheduler == null) {
      synchronized (SCHEDULER_LOCK) {
        if (_refreshScheduler == null) {
          _refreshScheduler = Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setNameFormat("protobuf-codegen-cache-refresh-%d")
                  .setDaemon(true)
                  .build());
        }
      }
    }
    return _refreshScheduler;
  }

  /** Closes the {@link URLClassLoader} backing {@code entry}'s method, if any, ignoring errors. */
  private static void closeClassLoader(CacheEntry entry) {
    if (entry == null) {
      return;
    }
    ClassLoader cl = entry._method.getDeclaringClass().getClassLoader();
    if (cl instanceof URLClassLoader) {
      try {
        ((URLClassLoader) cl).close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close URLClassLoader for protobuf codegen method", e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Build pipeline (JAR fetch → descriptor → codegen → compile → method)
  // ---------------------------------------------------------------------------

  private static Method buildDecodeMethod(String jarPath, String protoClassName, Set<String> fieldsToRead)
      throws Exception {
    ClassLoader protoMessageClsLoader = loadClass(jarPath);
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(protoMessageClsLoader, protoClassName);
    String codeGenCode = new MessageCodeGen().codegen(descriptor, fieldsToRead);
    Class<?> recordExtractor = compileClass(protoMessageClsLoader,
        MessageCodeGen.EXTRACTOR_PACKAGE_NAME + "." + MessageCodeGen.EXTRACTOR_CLASS_NAME, codeGenCode);
    return recordExtractor.getMethod(MessageCodeGen.EXTRACTOR_METHOD_NAME, byte[].class, GenericRow.class);
  }

  public static ClassLoader loadClass(String jarFilePath) {
    try {
      File file = ProtoBufUtils.getFileCopiedToLocal(jarFilePath);
      URL url = file.toURI().toURL();
      URL[] urls = new URL[]{url};
      return new URLClassLoader(urls);
    } catch (Exception e) {
      throw new RuntimeException("Error loading protobuf class", e);
    }
  }

  public static Class<?> compileClass(ClassLoader classloader, String className, String code)
      throws ClassNotFoundException {
    SimpleCompiler simpleCompiler = new SimpleCompiler();
    simpleCompiler.setParentClassLoader(classloader);
    try {
      simpleCompiler.cook(code);
    } catch (Throwable t) {
      throw new RuntimeException("Program cannot be compiled. This is a bug. Please file an issue.", t);
    }
    return simpleCompiler.getClassLoader().loadClass(className);
  }

  public static Descriptors.Descriptor getDescriptorForProtoClass(ClassLoader protoMessageClsLoader,
      String protoClassName)
      throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
    Class<? extends Message> updateMessage = (Class<Message>) protoMessageClsLoader.loadClass(protoClassName);
    return (Descriptors.Descriptor) updateMessage.getMethod("getDescriptor").invoke(null);
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /** Clears all static cache state. For use in tests only. */
  @VisibleForTesting
  static void clearCacheForTest() {
    REFRESH_TASKS.values().forEach(f -> f.cancel(false));
    REFRESH_TASKS.clear();
    METHOD_CACHE.clear();
  }
}
