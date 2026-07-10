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
package org.apache.pinot.controller.helix.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TopicPartitionId;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class PinotHelixResourceManagerLastLLCSegmentsTest {

  private static final String TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);

  /**
   * A realtime table can contain non-LLC-named segments (e.g. uploaded via batch ingestion) sitting in DONE state
   * alongside the LLC-named consuming/committed segments. {@code getLastLLCCompletedSegments} must skip those
   * uploaded segments rather than NPE when {@code LLCSegmentName.of(name)} returns {@code null}.
   */
  @Test
  public void testGetLastLLCCompletedSegmentsSkipsNonLLCNamedSegments() {
    long now = System.currentTimeMillis();
    int partitionId = 3;

    List<SegmentZKMetadata> segments = new ArrayList<>();
    // Two LLC-named DONE segments — sequence 0 and 1 for the same partition; sequence 1 is the latest.
    LLCSegmentName seq0 = new LLCSegmentName(TABLE_NAME, partitionId, 0, now);
    LLCSegmentName seq1 = new LLCSegmentName(TABLE_NAME, partitionId, 1, now);
    segments.add(doneSegment(seq0.getSegmentName()));
    segments.add(doneSegment(seq1.getSegmentName()));
    // An uploaded (non-LLC-named) segment in DONE state — must be ignored, not crash the method.
    segments.add(doneSegment("uploaded_segment_0"));

    PinotHelixResourceManager rm = mock(PinotHelixResourceManager.class);
    when(rm.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(segments);
    when(rm.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(mock(TableConfig.class));
    when(rm.getLastLLCCompletedSegments(REALTIME_TABLE_NAME)).thenCallRealMethod();

    Collection<String> lastCompleted = rm.getLastLLCCompletedSegments(REALTIME_TABLE_NAME);
    Set<String> actual = new HashSet<>(lastCompleted);
    assertEquals(actual, Set.of(seq1.getSegmentName()));
  }

  /**
   * On a multi-stream table, an old-format (4-part) segment name encodes a composite partition ID
   * (topicId * 10000 + partitionId) that must be decomposed via {@code hasMultipleStreams=true}. This test mixes
   * one old-format name (raw composite ID 10002, i.e. topicId=1/partitionId=2) with one new-format (5-part) name
   * built directly from {@code TopicPartitionId(1, 2)} for the same logical partition, at a higher sequence number.
   * This is the only kind of comparison that discriminates correct decomposition from the bug: if
   * {@code hasMultipleStreams} were incorrectly false, the old-format name would resolve to
   * {@code TopicPartitionId(0, 10002)} instead of {@code TopicPartitionId(1, 2)}, landing in a different bucket
   * than the new-format segment rather than losing to it as the lower-sequence entry in the same bucket.
   */
  @Test
  public void testGetLastLLCCompletedSegmentsDecomposesCompositePartitionIdOnMultiStreamTable() {
    long now = System.currentTimeMillis();
    // Old-format (4-part) name with composite raw ID for (topicId=1, partitionId=2): 1 * 10000 + 2 = 10002.
    LLCSegmentName oldFormatSeq0 = new LLCSegmentName(TABLE_NAME, 10002, 0, now);
    // New-format (5-part) name for the same logical partition (topicId=1, partitionId=2), higher sequence number.
    LLCSegmentName newFormatSeq1 =
        new LLCSegmentName(TABLE_NAME, new TopicPartitionId(1, 2), 1, now, true);

    List<SegmentZKMetadata> segments = new ArrayList<>();
    segments.add(doneSegment(oldFormatSeq0.getSegmentName()));
    segments.add(doneSegment(newFormatSeq1.getSegmentName()));

    TableConfig multiStreamTableConfig = mock(TableConfig.class);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(List.of(Map.of("streamType", "kafka"), Map.of("streamType", "kafka"))));
    when(multiStreamTableConfig.getIngestionConfig()).thenReturn(ingestionConfig);

    PinotHelixResourceManager rm = mock(PinotHelixResourceManager.class);
    when(rm.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(segments);
    when(rm.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(multiStreamTableConfig);
    when(rm.getLastLLCCompletedSegments(REALTIME_TABLE_NAME)).thenCallRealMethod();

    Collection<String> lastCompleted = rm.getLastLLCCompletedSegments(REALTIME_TABLE_NAME);
    Set<String> actual = new HashSet<>(lastCompleted);
    // Both names decompose to the same TopicPartitionId(1, 2) bucket; only the higher-sequence new-format segment
    // should win. If decomposition were skipped, the old-format segment would land in its own separate bucket
    // (TopicPartitionId(0, 10002)) and incorrectly also appear as a winner.
    assertEquals(actual, Set.of(newFormatSeq1.getSegmentName()));
  }

  private static SegmentZKMetadata doneSegment(String name) {
    SegmentZKMetadata md = new SegmentZKMetadata(name);
    md.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    return md;
  }
}
