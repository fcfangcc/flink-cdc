package org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionMetric {
    private final MetricGroup metricGroup;
    private final Map<TableId, CompactMetrics> tableIdMetricMap;
    public static final String NAMESPACE_GROUP_KEY = "namespace";
    public static final String SCHEMA_GROUP_KEY = "schema";
    public static final String TABLE_GROUP_KEY = "table";

    public CompactionMetric(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.tableIdMetricMap = new HashMap<>();
    }

    public Optional<CompactMetrics> getTableMetric(TableId tableId) {
        if (metricGroup == null) {
            return Optional.empty();
        }
        CompactMetrics metrics =
                tableIdMetricMap.computeIfAbsent(
                        tableId,
                        k -> {
                            MetricGroup tableIdMetricGroup =
                                    metricGroup
                                            .addGroup(NAMESPACE_GROUP_KEY, tableId.getNamespace())
                                            .addGroup(SCHEMA_GROUP_KEY, tableId.getSchemaName())
                                            .addGroup(TABLE_GROUP_KEY, tableId.getTableName());
                            return new CompactMetrics(tableIdMetricGroup);
                        });
        return Optional.of(metrics);
    }

    public static class CompactMetrics {
        private final AtomicInteger intervalTimes;
        private final Counter compactSuccessesTimes;
        private final Counter compactFailuresTimes;

        public static final String COMPACT_INTERVAL_TIMES = "CompactIntervalTimes";
        public static final String COMPACT_SUCCESSES_TIMES = "CompactSuccessesTimes";
        public static final String COMPACT_FAILURES_TIMES = "CompactFailuresTimes";

        public CompactMetrics(MetricGroup metricGroup) {
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
