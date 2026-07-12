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
package org.apache.pinot.controller.helix;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.ExponentialMovingAverage;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeConsumerMonitor extends ControllerPeriodicTask<RealtimeConsumerMonitor.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeConsumerMonitor.class);
  private static final int DEFAULT_TIMEOUT_MS = 10000;

  // Dual-EWMA lag trend detection, used to gate offset auto-reset on persistently rising availability lag instead of
  // transient spikes. Fast EWMA reacts quickly to recent lag; slow EWMA smooths over a longer window as a baseline.
  private static final double FAST_EWMA_ALPHA = 0.5;
  private static final double SLOW_EWMA_ALPHA = 0.1;
  // Trend is confirmed "rising" once fastEwma exceeds slowEwma by this multiplier for RISING_CONFIRMATION_TICKS
  // consecutive runs, and confirmed "recovered" once fastEwma drops back under slowEwma by RECOVERING_RATIO for
  // RECOVERING_CONFIRMATION_TICKS consecutive runs. The gap between the two ratios (and requiring consecutive ticks
  // on both sides) gives the detector hysteresis so it doesn't flap on noisy lag samples.
  private static final double RISING_RATIO = 1.5;
  private static final double RECOVERING_RATIO = 1.1;
  private static final int RISING_CONFIRMATION_TICKS = 3;
  private static final int RECOVERING_CONFIRMATION_TICKS = 3;
  // Below this availability-lag threshold (ms), the ratio check is skipped entirely so that trivial jitter near
  // zero lag (e.g. 10ms → 20ms, which exceeds RISING_RATIO but is operationally meaningless) cannot accumulate
  // confirmation ticks.
  private static final long MIN_LAG_FOR_TREND_EVALUATION_MS = 30_000L;
  // If no lag sample has been observed for this many monitor-run intervals, treat trend data as stale and clear
  // _confirmedRising rather than letting a frozen-true state gate offset-reset decisions indefinitely.
  private static final int STALENESS_TICKS = 5;

  private final ConsumingSegmentInfoReader _consumingSegmentInfoReader;
  // Table -> partition -> dual-EWMA trend state, used to gate offset auto-reset on persistently rising lag.
  // In-memory only and scoped to whichever controller currently leads the table: on leadership handoff the new
  // leader starts cold and re-accumulates, which only delays (re-)confirmation and does not affect correctness.
  private final Map<String, Map<Integer, PartitionTrendState>> _tableToPartitionTrendState = new ConcurrentHashMap<>();
  // Tracks which partitions received a lag sample this tick, so partitions that didn't (e.g. NOT_CALCULATED) can
  // have their staleness counter incremented.
  private final Map<String, Map<Integer, Boolean>> _tablePartitionUpdatedThisTick = new ConcurrentHashMap<>();

  @VisibleForTesting
  public RealtimeConsumerMonitor(ControllerConf controllerConf, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics,
      ConsumingSegmentInfoReader consumingSegmentInfoReader) {
    super("RealtimeConsumerMonitor", controllerConf.getRealtimeConsumerMonitorRunFrequency(),
        controllerConf.getRealtimeConsumerMonitorInitialDelayInSeconds(),
        controllerConf.getRealtimeConsumerMonitorCronExpression(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _consumingSegmentInfoReader = consumingSegmentInfoReader;
  }

  public RealtimeConsumerMonitor(ControllerConf controllerConf, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics,
      ExecutorService executorService) {
    this(controllerConf, pinotHelixResourceManager, leadControllerManager, controllerMetrics,
        new ConsumingSegmentInfoReader(executorService, new BasicHttpClientConnectionManager(),
            pinotHelixResourceManager));
  }

  @Override
  protected void processTable(String tableNameWithType) {
    if (!TableType.REALTIME.equals(TableNameBuilder.getTableTypeFromTableName(tableNameWithType))) {
      return;
    }
    try {
      ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap segmentsInfoMap =
          _consumingSegmentInfoReader.getConsumingSegmentsInfo(tableNameWithType, DEFAULT_TIMEOUT_MS);
      Map<String, List<Long>> partitionToLagSet = new HashMap<>();
      Map<String, List<Long>> partitionToAvailabilityLagSet = new HashMap<>();

      for (List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> info
          : segmentsInfoMap._segmentToConsumingInfoMap.values()) {
        info.forEach(segment -> {
          segment._partitionOffsetInfo._recordsLagMap.forEach((k, v) -> {
            if (!PartitionLagState.NOT_CALCULATED.equals(v)) {
              try {
                long recordsLag = Long.parseLong(v);
                partitionToLagSet.computeIfAbsent(k, k1 -> new ArrayList<>()).add(recordsLag);
              } catch (NumberFormatException nfe) {
                // skip this as we are unable to parse the lag string
              }
            }
          });
          segment._partitionOffsetInfo._availabilityLagMap.forEach((k, v) -> {
            if (!PartitionLagState.NOT_CALCULATED.equals(v)) {
              try {
                long availabilityLagMs = Long.parseLong(v);
                partitionToAvailabilityLagSet.computeIfAbsent(k, k1 -> new ArrayList<>()).add(availabilityLagMs);
              } catch (NumberFormatException nfe) {
                // skip this as we are unable to parse the lag string
              }
            }
          });
        });
      }
      partitionToLagSet.forEach((partition, lagSet) -> {
        _controllerMetrics.setValueOfPartitionGauge(tableNameWithType, Integer.parseInt(partition),
            ControllerGauge.MAX_RECORDS_LAG, Collections.max(lagSet));
      });

      partitionToAvailabilityLagSet.forEach((partition, lagSet) -> {
        long maxAvailabilityLagMs = Collections.max(lagSet);
        int partitionId = Integer.parseInt(partition);
        _controllerMetrics.setValueOfPartitionGauge(tableNameWithType, partitionId,
            ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS, maxAvailabilityLagMs);
        updateLagTrend(tableNameWithType, partitionId, maxAvailabilityLagMs);
      });

      // Increment staleness counter for partitions that had prior trend state but didn't report lag this tick.
      Map<Integer, PartitionTrendState> trendStates = _tableToPartitionTrendState.get(tableNameWithType);
      Map<Integer, Boolean> updatedPartitions = _tablePartitionUpdatedThisTick.remove(tableNameWithType);
      if (trendStates != null) {
        for (Map.Entry<Integer, PartitionTrendState> entry : trendStates.entrySet()) {
          if (updatedPartitions == null || !updatedPartitions.containsKey(entry.getKey())) {
            entry.getValue().incrementStaleness();
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to fetch consuming segments info. Unable to update table consumption status metrics");
    }
  }

  private void updateLagTrend(String tableNameWithType, int partition, long maxAvailabilityLagMs) {
    Map<Integer, PartitionTrendState> partitionTrendState =
        _tableToPartitionTrendState.computeIfAbsent(tableNameWithType, k -> new ConcurrentHashMap<>());
    PartitionTrendState trendState = partitionTrendState.computeIfAbsent(partition,
        k -> new PartitionTrendState(FAST_EWMA_ALPHA, SLOW_EWMA_ALPHA, maxAvailabilityLagMs));
    trendState.update(maxAvailabilityLagMs);
    _tablePartitionUpdatedThisTick.computeIfAbsent(tableNameWithType, k -> new ConcurrentHashMap<>())
        .put(partition, Boolean.TRUE);
  }

  /**
   * Returns whether the ingestion lag trend for the given table partition has been confirmed as persistently
   * rising by the dual-EWMA detector, i.e. lag is trending up rather than a transient spike. Used as a precondition
   * for Kafka offset auto-reset so that resets are not triggered while Pinot is still able to catch up on its own.
   *
   * @param tableNameWithType table to check, including type suffix
   * @param partition partition id within the table
   * @return true only if a persistently-rising trend has been confirmed by this controller leader. Returns false
   *         both when no lag samples have been observed yet for this partition (e.g. right after leadership
   *         handoff) and when the trend has been observed but is not currently confirmed as rising; callers should
   *         not treat false as an indication that lag is known to be stable.
   */
  public boolean isLagTrendConfirmedRising(String tableNameWithType, int partition) {
    Map<Integer, PartitionTrendState> partitionTrendState = _tableToPartitionTrendState.get(tableNameWithType);
    if (partitionTrendState == null) {
      return false;
    }
    PartitionTrendState trendState = partitionTrendState.get(partition);
    return trendState != null && trendState.isConfirmedRising();
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    for (String tableNameWithType : tableNamesWithType) {
      _tableToPartitionTrendState.remove(tableNameWithType);
      _tablePartitionUpdatedThisTick.remove(tableNameWithType);
    }
  }

  private static final class PartitionTrendState {
    private final ExponentialMovingAverage _fastEwma;
    private final ExponentialMovingAverage _slowEwma;
    private int _risingTicks = 0;
    private int _recoveringTicks = 0;
    private int _ticksSinceLastUpdate = 0;
    private volatile boolean _confirmedRising = false;

    // Seed both EWMAs from the first observed sample so fast == slow initially. Seeding from 0 instead would make
    // the fast EWMA converge toward the true (possibly flat, non-rising) lag several times faster than the slow
    // EWMA, pushing their ratio above RISING_RATIO for several ticks on pure cold-start convergence rather than any
    // actual upward trend - exactly when a fresh PartitionTrendState is created, e.g. right after leadership
    // handoff, when a false "confirmed rising" signal would be most damaging.
    private PartitionTrendState(double fastAlpha, double slowAlpha, long firstValue) {
      _fastEwma = new ExponentialMovingAverage(fastAlpha, 0, 0, firstValue, null);
      _slowEwma = new ExponentialMovingAverage(slowAlpha, 0, 0, firstValue, null);
    }

    // Called by processTable when a partition reports a valid availability-lag value.
    private synchronized void update(long value) {
      _ticksSinceLastUpdate = 0;
      double fast = _fastEwma.compute(value);
      double slow = _slowEwma.compute(value);

      if (slow < MIN_LAG_FOR_TREND_EVALUATION_MS) {
        _risingTicks = 0;
        _recoveringTicks = 0;
        _confirmedRising = false;
        return;
      }

      if (fast > slow * RISING_RATIO) {
        _risingTicks++;
        _recoveringTicks = 0;
        if (_risingTicks >= RISING_CONFIRMATION_TICKS) {
          _confirmedRising = true;
        }
      } else if (fast < slow * RECOVERING_RATIO) {
        _recoveringTicks++;
        _risingTicks = 0;
        if (_recoveringTicks >= RECOVERING_CONFIRMATION_TICKS) {
          _confirmedRising = false;
        }
      }
      // Else: in between the rising and recovering ratios. Leave the streak counts and confirmed state untouched,
      // so a single noisy in-between sample doesn't reset progress toward confirming (or un-confirming) a trend.
    }

    // Called when a partition did not report availability lag this tick (e.g. NOT_CALCULATED). If no update
    // arrives for STALENESS_TICKS consecutive ticks, _confirmedRising is cleared so stale state cannot gate
    // offset-reset decisions indefinitely.
    private synchronized void incrementStaleness() {
      _ticksSinceLastUpdate++;
      if (_ticksSinceLastUpdate >= STALENESS_TICKS) {
        _confirmedRising = false;
        _risingTicks = 0;
        _recoveringTicks = 0;
      }
    }

    private boolean isConfirmedRising() {
      return _confirmedRising;
    }
  }

  public static final class Context { }
}
