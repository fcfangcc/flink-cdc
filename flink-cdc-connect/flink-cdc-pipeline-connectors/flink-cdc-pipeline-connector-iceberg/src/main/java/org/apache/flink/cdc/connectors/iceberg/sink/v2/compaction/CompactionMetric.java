/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** A class to manage compaction metrics. */
public class CompactionMetric implements Serializable {
    private final MetricGroup metricGroup;
    private final Map<TableId, CompactMetricGroup> tableIdMetricMap;
    public static final String NAMESPACE_GROUP_KEY = "namespace";
    public static final String SCHEMA_GROUP_KEY = "schema";
    public static final String TABLE_GROUP_KEY = "table";

    public CompactionMetric(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.tableIdMetricMap = new HashMap<>();
    }

    public Optional<CompactMetricGroup> getTableMetric(TableId tableId) {
        if (metricGroup == null) {
            return Optional.empty();
        }
        CompactMetricGroup metrics =
                tableIdMetricMap.computeIfAbsent(
                        tableId,
                        k -> {
                            MetricGroup tableIdMetricGroup =
                                    metricGroup
                                            .addGroup(SCHEMA_GROUP_KEY, tableId.getSchemaName())
                                            .addGroup(TABLE_GROUP_KEY, tableId.getTableName());
                            if (tableId.getNamespace() != null) {
                                tableIdMetricGroup =
                                        tableIdMetricGroup.addGroup(
                                                NAMESPACE_GROUP_KEY, tableId.getNamespace());
                            }

                            return new CompactMetricGroup(tableIdMetricGroup);
                        });
        return Optional.of(metrics);
    }

    /** A class to commit compaction metrics. */
    public static class CompactMetricGroup {
        private final AtomicInteger intervalTimes;
        private final Counter compactSuccessesTimes;
        private final Counter compactFailuresTimes;

        public static final String COMPACT_INTERVAL_TIMES = "compactIntervalTimes";
        public static final String COMPACT_SUCCESSES_TIMES = "compactSuccesses";
        public static final String COMPACT_FAILURES_TIMES = "compactFailures";

        public CompactMetricGroup(MetricGroup metricGroup) {
            this.intervalTimes = new AtomicInteger(0);
            metricGroup.gauge(COMPACT_INTERVAL_TIMES, this.intervalTimes::intValue);
            this.compactSuccessesTimes = metricGroup.counter(COMPACT_SUCCESSES_TIMES);
            this.compactFailuresTimes = metricGroup.counter(COMPACT_FAILURES_TIMES);
        }

        public void setIntervalTimes(int times) {
            intervalTimes.set(times);
        }

        public void incCompactSuccessesTimes() {
            compactSuccessesTimes.inc();
        }

        public void incCompactFailuresTimes() {
            compactFailuresTimes.inc();
        }
    }
}
