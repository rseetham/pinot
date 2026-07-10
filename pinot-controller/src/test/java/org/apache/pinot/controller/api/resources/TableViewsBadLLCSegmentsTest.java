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
package org.apache.pinot.controller.api.resources;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class TableViewsBadLLCSegmentsTest {

  private static final String TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
  private static final String HELIX_CLUSTER_NAME = "testCluster";

  /**
   * When {@code getTableConfig} returns null (e.g. a transient ZK read race), {@code getBadLLCSegmentsPerPartition}
   * must not NPE while resolving {@code hasMultipleStreams} — it should fall back to single-stream (old-format)
   * partition ID handling.
   */
  @Test
  public void testGetBadLLCSegmentsPerPartitionHandlesNullTableConfig() throws Exception {
    LLCSegmentName badSegment = new LLCSegmentName(TABLE_NAME, 3, 0, System.currentTimeMillis());

    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(badSegment.getSegmentName(), "Server1", "ONLINE");
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setStateMap(badSegment.getSegmentName(), Map.of("Server1", "ERROR"));

    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(externalView);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    when(resourceManager.getHelixClusterName()).thenReturn(HELIX_CLUSTER_NAME);
    when(resourceManager.getExistingTableNamesWithType(TABLE_NAME, TableType.REALTIME))
        .thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(null);

    TableViews tableViews = new TableViews();
    setField(tableViews, "_pinotHelixResourceManager", resourceManager);

    String result = tableViews.getBadLLCSegmentsPerPartition(TABLE_NAME, mock(HttpHeaders.class));
    Map<String, Object> partitionMap = JsonUtils.stringToObject(result, Map.class);
    // Single-stream fallback: TopicPartitionId(0, 3).toString() is "0:3".
    assertTrue(partitionMap.containsKey("0:3"));
  }

  /**
   * On a multi-stream table, an old-format (4-part) bad segment name encodes a composite partition ID
   * (topicId * 10000 + partitionId) that must be decomposed via {@code hasMultipleStreams=true} so the result is
   * keyed by the decomposed {@code TopicPartitionId} rather than the raw composite integer.
   */
  @Test
  public void testGetBadLLCSegmentsPerPartitionDecomposesCompositePartitionIdOnMultiStreamTable() throws Exception {
    // Old-format composite raw ID for (topicId=1, partitionId=2) is 1 * 10000 + 2 = 10002.
    LLCSegmentName badSegment = new LLCSegmentName(TABLE_NAME, 10002, 0, System.currentTimeMillis());

    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(badSegment.getSegmentName(), "Server1", "ONLINE");
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setStateMap(badSegment.getSegmentName(), Map.of("Server1", "ERROR"));

    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(externalView);

    TableConfig multiStreamTableConfig = mock(TableConfig.class);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(List.of(Map.of("streamType", "kafka"), Map.of("streamType", "kafka"))));
    when(multiStreamTableConfig.getIngestionConfig()).thenReturn(ingestionConfig);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    when(resourceManager.getHelixClusterName()).thenReturn(HELIX_CLUSTER_NAME);
    when(resourceManager.getExistingTableNamesWithType(TABLE_NAME, TableType.REALTIME))
        .thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(multiStreamTableConfig);

    TableViews tableViews = new TableViews();
    setField(tableViews, "_pinotHelixResourceManager", resourceManager);

    String result = tableViews.getBadLLCSegmentsPerPartition(TABLE_NAME, mock(HttpHeaders.class));
    Map<String, Object> partitionMap = JsonUtils.stringToObject(result, Map.class);
    // Decomposed TopicPartitionId(1, 2).toString() is "1:2", not the raw composite "10002".
    assertTrue(partitionMap.containsKey("1:2"));
    assertFalse(partitionMap.containsKey("10002"));
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
