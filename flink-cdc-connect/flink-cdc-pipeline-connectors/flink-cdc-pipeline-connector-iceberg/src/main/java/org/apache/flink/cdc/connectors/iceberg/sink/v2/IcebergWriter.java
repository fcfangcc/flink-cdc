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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.RowDataUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link SinkWriter} for Apache Iceberg. */
public class IcebergWriter implements CommittingSinkWriter<Event, WriteResultWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergWriter.class);

    public static final String DEFAULT_FILE_FORMAT = "parquet";

    public static final long DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;

    private Map<TableId, RowDataTaskWriterFactory> writerFactoryMap;

    private Map<TableId, TaskWriter<RowData>> writerMap;

    private List<TemplateWriter> templateWriters;

    private Map<TableId, TableSchemaWrapper> schemaMap;

    private Catalog catalog;

    private final int taskId;

    private final int attemptId;

    private final ZoneId zoneId;

    private static class TemplateWriter {
        private final TableId tableId;
        private final TaskWriter<RowData> writer;
        private final long timestamp;
        private final long eventId;

        private TemplateWriter(long eventId, TableId tableId, TaskWriter<RowData> writer) {
            this.eventId = eventId;
            this.tableId = tableId;
            this.writer = writer;
            this.timestamp = Instant.now().toEpochMilli();
        }
    }

    public IcebergWriter(
            Map<String, String> catalogOptions, int taskId, int attemptId, ZoneId zoneId) {
        catalog =
                CatalogUtil.buildIcebergCatalog(
                        this.getClass().getSimpleName(), catalogOptions, new Configuration());
        writerFactoryMap = new HashMap<>();
        writerMap = new HashMap<>();
        schemaMap = new HashMap<>();
        this.taskId = taskId;
        this.attemptId = attemptId;
        this.zoneId = zoneId;
        this.templateWriters = new ArrayList<>();
    }

    private void saveAndRemoveCurrentWriter(TableId tableId, int eventId) {
        LOGGER.info("Got saveAndRemoveCurrentWriter: {}", tableId.identifier());
        TaskWriter<RowData> writer = writerMap.remove(tableId);
        if (writer != null) {
            this.templateWriters.add(new TemplateWriter(eventId, tableId, writer));
        }
        writerFactoryMap.remove(tableId);
    }

    @Override
    public Collection<WriteResultWrapper> prepareCommit() throws IOException, InterruptedException {
        return getWriteResult();
    }

    private RowDataTaskWriterFactory getRowDataTaskWriterFactory(TableId tableId) {
        Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
        RowType rowType = FlinkSchemaUtil.convert(table.schema());
        RowDataTaskWriterFactory rowDataTaskWriterFactory =
                new RowDataTaskWriterFactory(
                        table,
                        rowType,
                        DEFAULT_MAX_FILE_SIZE,
                        FileFormat.fromString(DEFAULT_FILE_FORMAT),
                        new HashMap<>(),
                        new ArrayList<>(table.schema().identifierFieldIds()),
                        true);
        rowDataTaskWriterFactory.initialize(taskId, attemptId);
        return rowDataTaskWriterFactory;
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableId tableId = dataChangeEvent.tableId();
            writerFactoryMap.computeIfAbsent(tableId, this::getRowDataTaskWriterFactory);
            TaskWriter<RowData> writer =
                    writerMap.computeIfAbsent(
                            tableId, tableId1 -> writerFactoryMap.get(tableId1).create());
            TableSchemaWrapper tableSchemaWrapper = schemaMap.get(tableId);
            RowData rowData =
                    RowDataUtils.convertDataChangeEventToRowData(
                            dataChangeEvent, tableSchemaWrapper.getFieldGetters());
            writer.write(rowData);
        } else {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            LOGGER.info(
                    "Got schema change event tableId: {}, class:{} -> {}",
                    tableId.getTableName(),
                    schemaChangeEvent.getClass().getName(),
                    schemaChangeEvent);
            TableSchemaWrapper tableSchemaWrapper = schemaMap.get(tableId);
            Schema newSchema;
            if (tableSchemaWrapper != null) {
                newSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                tableSchemaWrapper.getSchema(), schemaChangeEvent);
                // todo: 判断是否支持的事件/是否需要flush的事件
                saveAndRemoveCurrentWriter(tableId, schemaChangeEvent.hashCode());
            } else {
                newSchema = SchemaUtils.applySchemaChangeEvent(null, schemaChangeEvent);
            }
            schemaMap.put(tableId, new TableSchemaWrapper(newSchema, zoneId));
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        // Notice: flush method may be called many times during one checkpoint.
        // do nothing as Write will write buffer to file.
        LOGGER.info("Got flush event with taskId: {}, endOfInput:{}", this.taskId, endOfInput);
    }

    private List<WriteResultWrapper> getWriteResult() throws IOException {
        List<WriteResultWrapper> writeResults = new ArrayList<>();
        for (TemplateWriter entry : templateWriters) {
            WriteResultWrapper writeResultWrapper =
                    new WriteResultWrapper(entry.writer.complete(), entry.tableId);
            writeResultWrapper.setTimestamp(entry.timestamp);
            writeResultWrapper.setEventId(entry.eventId);
            writeResults.add(writeResultWrapper);
            LOGGER.info(
                    "flush complete file immediately taskId: {}, table: {}, writeResult: {}",
                    this.taskId,
                    entry.tableId.getTableName(),
                    writeResultWrapper.buildDescription());
        }

        for (Map.Entry<TableId, TaskWriter<RowData>> entry : writerMap.entrySet()) {
            WriteResultWrapper writeResultWrapper =
                    new WriteResultWrapper(entry.getValue().complete(), entry.getKey());
            writeResults.add(writeResultWrapper);
            LOGGER.info(
                    "flush complete file taskId: {}, table: {}, writeResult: {}",
                    this.taskId,
                    entry.getKey().getTableName(),
                    writeResultWrapper.buildDescription());
        }

        writerMap.clear();
        writerFactoryMap.clear();
        templateWriters.clear();
        return writeResults;
    }

    @Override
    public void writeWatermark(Watermark watermark) {}

    @Override
    public void close() throws Exception {
        if (schemaMap != null) {
            schemaMap.clear();
            schemaMap = null;
        }

        if (writerMap != null) {
            for (TaskWriter<RowData> writer : writerMap.values()) {
                writer.close();
            }
            writerMap.clear();
            writerMap = null;
        }

        for (TemplateWriter templateWriter : templateWriters) {
            templateWriter.writer.close();
        }
        templateWriters.clear();
        templateWriters = null;

        if (writerFactoryMap != null) {
            writerFactoryMap.clear();
            writerFactoryMap = null;
        }

        catalog = null;
    }
}
