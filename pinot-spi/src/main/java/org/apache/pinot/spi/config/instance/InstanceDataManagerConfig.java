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
package org.apache.pinot.spi.config.instance;

import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;


public interface InstanceDataManagerConfig {

  PinotConfiguration getConfig();

  String getInstanceId();

  String getInstanceDataDir();

  String getConsumerDir();

  String getInstanceSegmentTarDir();

  String getTableDataManagerProviderClass();

  String getSegmentStoreUri();

  String getConsumerClientIdSuffix();

  ReadMode getReadMode();

  String getSegmentFormatVersion();

  String getAvgMultiValueCount();

  boolean isRealtimeOffHeapAllocation();

  boolean isDirectRealtimeOffHeapAllocation();

  boolean shouldReloadConsumingSegment();

  int getMaxParallelRefreshThreads();

  int getMaxSegmentPreloadThreads();

  int getMaxParallelSegmentBuilds();

  int getMaxParallelSegmentDownloads();

  String getSegmentDirectoryLoader();

  long getErrorCacheSize();

  boolean isStreamSegmentDownloadUntar();

  long getStreamSegmentDownloadUntarRateLimit();

  int getDeletedTablesCacheTtlMinutes();

  int getDeletedSegmentsCacheSize();

  int getDeletedSegmentsCacheTtlMinutes();

  String getSegmentPeerDownloadScheme();

  PinotConfiguration getUpsertConfig();

  PinotConfiguration getDedupConfig();

  PinotConfiguration getAuthConfig();

  Map<String, Map<String, String>> getTierConfigs();

  boolean isUploadSegmentToDeepStore();
}
