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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

/** A {@link Committer} for Apache Iceberg. */
public class IcebergCommitter implements Committer<WriteResultWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCommitter.class);

    public static final String NAMESPACE_GROUP_KEY = "namespace";

    public static final String SCHEMA_GROUP_KEY = "schema";

    public static final String TABLE_GROUP_KEY = "table";

    private final Catalog catalog;

    private final SinkCommitterMetricGroup metricGroup;

    private final Map<TableId, TableMetric> tableIdMetricMap;

    public IcebergCommitter(Map<String, String> catalogOptions) {
        this(catalogOptions, null);
    }

    public IcebergCommitter(
            Map<String, String> catalogOptions, SinkCommitterMetricGroup metricGroup) {
        this.catalog =
                CatalogUtil.buildIcebergCatalog(
                        this.getClass().getSimpleName(), catalogOptions, new Configuration());
        this.metricGroup = metricGroup;
        this.tableIdMetricMap = new HashMap<>();
    }

    @Override
    public void commit(Collection<CommitRequest<WriteResultWrapper>> collection) {
        List<WriteResultWrapper> results =
                collection.stream().map(CommitRequest::getCommittable).collect(toList());
        commit(results);
    }

    private void commit(List<WriteResultWrapper> writeResultWrappers) {
        LOGGER.info("start flush and commit sink");
        Map<TableId, List<WriteResultWrapper>> resultMap = new HashMap<>();
        Map<TableId, LinkedHashMap<Long, List<WriteResultWrapper>>> eventResultMap =
                new HashMap<>();
        writeResultWrappers.sort(Comparator.comparingLong(WriteResultWrapper::getTimestamp));
        for (WriteResultWrapper writeResultWrapper : writeResultWrappers) {
            List<WriteResultWrapper> writeResult;
            Long eventId = writeResultWrapper.getEventId();
            if (eventId == null) {
                writeResult =
                        resultMap.computeIfAbsent(
                                writeResultWrapper.getTableId(), k -> new ArrayList<>());
            } else {

                writeResult =
                        eventResultMap
                                .computeIfAbsent(
                                        writeResultWrapper.getTableId(), k -> new LinkedHashMap<>())
                                .computeIfAbsent(eventId, k -> new ArrayList<>());
            }
            writeResult.add(writeResultWrapper);

            LOGGER.info(writeResultWrapper.buildDescription());
        }

        for (Map.Entry<TableId, LinkedHashMap<Long, List<WriteResultWrapper>>> entry1 :
                eventResultMap.entrySet()) {
            for (List<WriteResultWrapper> eventResults : entry1.getValue().values()) {
                flushWriteResults(entry1.getKey(), eventResults);
            }
        }

        for (Map.Entry<TableId, List<WriteResultWrapper>> entry : resultMap.entrySet()) {
            flushWriteResults(entry.getKey(), entry.getValue());
        }
    }

    private void flushWriteResults(TableId tableId, List<WriteResultWrapper> results) {
        List<DataFile> dataFiles =
                results.stream()
                        .map(WriteResultWrapper::getWriteResult)
                        .filter(payload -> payload.dataFiles() != null)
                        .flatMap(payload -> Arrays.stream(payload.dataFiles()))
                        .filter(dataFile -> dataFile.recordCount() > 0)
                        .collect(toList());
        List<DeleteFile> deleteFiles =
                results.stream()
                        .map(WriteResultWrapper::getWriteResult)
                        .filter(payload -> payload.deleteFiles() != null)
                        .flatMap(payload -> Arrays.stream(payload.deleteFiles()))
                        .filter(deleteFile -> deleteFile.recordCount() > 0)
                        .collect(toList());
        commitSnapshot(tableId, dataFiles, deleteFiles);
    }

    private void commitSnapshot(
            TableId tableId, List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
        Optional<TableMetric> tableMetric = getTableMetric(tableId);
        tableMetric.ifPresent(TableMetric::increaseCommitTimes);

        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            LOGGER.info(String.format("Nothing to commit to table %s, skipping", table.name()));
        } else {
            if (deleteFiles.isEmpty()) {
                AppendFiles append = table.newAppend();
                dataFiles.forEach(append::appendFile);
                append.commit();
            } else {
                RowDelta delta = table.newRowDelta();
                dataFiles.forEach(delta::addRows);
                deleteFiles.forEach(delta::addDeletes);
                delta.commit();
            }
        }
    }

    private Optional<TableMetric> getTableMetric(TableId tableId) {
        if (tableIdMetricMap.containsKey(tableId)) {
            return Optional.of(tableIdMetricMap.get(tableId));
        } else {
            if (metricGroup == null) {
                return Optional.empty();
            }
            MetricGroup tableIdMetricGroup =
                    metricGroup
                            .addGroup(NAMESPACE_GROUP_KEY, tableId.getNamespace())
                            .addGroup(SCHEMA_GROUP_KEY, tableId.getSchemaName())
                            .addGroup(TABLE_GROUP_KEY, tableId.getTableName());
            TableMetric tableMetric = new TableMetric(tableIdMetricGroup);
            tableIdMetricMap.put(tableId, tableMetric);
            return Optional.of(tableMetric);
        }
    }

    @Override
    public void close() {}
}
