/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.JsonUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSystemSplit;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetricsUtil.ReadableColMetricsStruct;
import org.apache.iceberg.MetricsUtil.ReadableMetricsStruct;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static io.trino.plugin.iceberg.IcebergUtil.getPartitionColumnType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class FilesTable
        implements SystemTable
{
    private static final JsonFactory JSON_FACTORY = JsonUtils.jsonFactoryBuilder().build();

    private static final String CONTENT_COLUMN_NAME = "content";
    private static final String FILE_PATH_COLUMN_NAME = "file_path";
    private static final String FILE_FORMAT_COLUMN_NAME = "file_format";
    private static final String SPEC_ID_COLUMN_NAME = "spec_id";
    private static final String PARTITION_COLUMN_NAME = "partition";
    private static final String RECORD_COUNT_COLUMN_NAME = "record_count";
    private static final String FILE_SIZE_IN_BYTES_COLUMN_NAME = "file_size_in_bytes";
    private static final String COLUMN_SIZES_COLUMN_NAME = "column_sizes";
    private static final String VALUE_COUNTS_COLUMN_NAME = "value_counts";
    private static final String NULL_VALUE_COUNTS_COLUMN_NAME = "null_value_counts";
    private static final String NAN_VALUE_COUNTS_COLUMN_NAME = "nan_value_counts";
    private static final String LOWER_BOUNDS_COLUMN_NAME = "lower_bounds";
    private static final String UPPER_BOUNDS_COLUMN_NAME = "upper_bounds";
    private static final String KEY_METADATA_COLUMN_NAME = "key_metadata";
    private static final String SPLIT_OFFSETS_COLUMN_NAME = "split_offsets";
    private static final String EQUALITY_IDS_COLUMN_NAME = "equality_ids";
    private static final String SORT_ORDER_ID_COLUMN_NAME = "sort_order_id";
    private static final String READABLE_METRICS_COLUMN_NAME = "readable_metrics";
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;
    private final List<io.trino.spi.type.Type> columnTypes;
    private final ExecutorService executor;

    public FilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId, ExecutorService executor)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");

        List<PartitionField> partitionFields = PartitionsTable.getAllPartitionFields(icebergTable);
        Optional<IcebergPartitionColumn> partitionColumnType = getPartitionColumnType(partitionFields, icebergTable.schema(), typeManager);

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.add(new ColumnMetadata(CONTENT_COLUMN_NAME, INTEGER));
        columns.add(new ColumnMetadata(FILE_PATH_COLUMN_NAME, VARCHAR));
        columns.add(new ColumnMetadata(FILE_FORMAT_COLUMN_NAME, VARCHAR));
        columns.add(new ColumnMetadata(SPEC_ID_COLUMN_NAME, INTEGER));
        partitionColumnType.ifPresent(type -> columns.add(new ColumnMetadata(PARTITION_COLUMN_NAME, type.rowType())));
        columns.add(new ColumnMetadata(RECORD_COUNT_COLUMN_NAME, BIGINT));
        columns.add(new ColumnMetadata(FILE_SIZE_IN_BYTES_COLUMN_NAME, BIGINT));
        columns.add(new ColumnMetadata(COLUMN_SIZES_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(NULL_VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(NAN_VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(LOWER_BOUNDS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))));
        columns.add(new ColumnMetadata(UPPER_BOUNDS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))));
        columns.add(new ColumnMetadata(KEY_METADATA_COLUMN_NAME, VARBINARY));
        columns.add(new ColumnMetadata(SPLIT_OFFSETS_COLUMN_NAME, new ArrayType(BIGINT)));
        columns.add(new ColumnMetadata(EQUALITY_IDS_COLUMN_NAME, new ArrayType(INTEGER)));
        columns.add(new ColumnMetadata(SORT_ORDER_ID_COLUMN_NAME, INTEGER));
        columns.add(new ColumnMetadata(READABLE_METRICS_COLUMN_NAME, typeManager.getType(new TypeSignature(JSON))));

        List<ColumnMetadata> partitionColumns = columns.build();

        this.tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), partitionColumns);
        this.columnTypes = partitionColumns.stream().map(ColumnMetadata::getType).toList();
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.ALL_NODES;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public Optional<ConnectorSplitSource> splitSource(ConnectorSession connectorSession, TupleDomain<ColumnHandle> constraint)
    {
        TableScan scan = icebergTable.newScan()
                .includeColumnStats()
                .planWith(executor);

        snapshotId.ifPresent(scan::useSnapshot);

        return Optional.of(new FilesTableSplitSource(
                scan.snapshot().allManifests(icebergTable.io()).iterator(),
                SchemaParser.toJson(icebergTable.schema()),
                icebergTable.specs().entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        kv -> PartitionSpecParser.toJson(kv.getValue()))),
                columnTypes));
    }

    public record FilesTableSplit(
            String manifestFileEncoded,
            String schemaJson,
            Map<Integer, String> partitionSpecsByIdJson,
            List<io.trino.spi.type.Type> columnTypes)
            implements ConnectorSystemSplit
    {
    }

    public static class FilesTableSplitSource
            implements ConnectorSplitSource
    {
        private final String schemaJson;
        private final Map<Integer, String> partitionSpecsByIdJson;
        private final List<io.trino.spi.type.Type> columnTypes;
        private final Iterator<ManifestFile> manifestFileItr;

        public FilesTableSplitSource(
                Iterator<ManifestFile> manifestFileItr,
                String schemaJson,
                Map<Integer, String> partitionSpecsByIdJson,
                List<io.trino.spi.type.Type> columnTypes)
        {
            this.schemaJson = requireNonNull(schemaJson, "schemaJson is null");
            this.partitionSpecsByIdJson = requireNonNull(partitionSpecsByIdJson, "partitionSpecsByIdJson is null");
            this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
            this.manifestFileItr = requireNonNull(manifestFileItr, "manifestFileItr is null");
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            List<ConnectorSplit> splits = new ArrayList<>();

            while (manifestFileItr.hasNext() && splits.size() < maxSize) {
                try {
                    splits.add(new FilesTableSplit(
                            Base64.getEncoder().encodeToString(ManifestFiles.encode(manifestFileItr.next())),
                            schemaJson,
                            partitionSpecsByIdJson,
                            columnTypes));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return completedFuture(new ConnectorSplitBatch(splits, !manifestFileItr.hasNext()));
        }

        @Override
        public void close()
        {
            // do nothing
        }

        @Override
        public boolean isFinished()
        {
            return manifestFileItr != null && !manifestFileItr.hasNext();
        }
    }

    static String toJson(ReadableMetricsStruct readableMetrics, List<Types.NestedField> primitiveFields)
    {
        StringWriter writer = new StringWriter();
        try {
            JsonGenerator generator = JSON_FACTORY.createGenerator(writer);
            generator.writeStartObject();

            for (int i = 0; i < readableMetrics.size(); i++) {
                Types.NestedField field = primitiveFields.get(i);
                generator.writeFieldName(field.name());

                generator.writeStartObject();
                ReadableColMetricsStruct columnMetrics = readableMetrics.get(i, ReadableColMetricsStruct.class);

                generator.writeFieldName("column_size");
                Long columnSize = columnMetrics.get(0, Long.class);
                if (columnSize == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(columnSize);
                }

                generator.writeFieldName("value_count");
                Long valueCount = columnMetrics.get(1, Long.class);
                if (valueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(valueCount);
                }

                generator.writeFieldName("null_value_count");
                Long nullValueCount = columnMetrics.get(2, Long.class);
                if (nullValueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(nullValueCount);
                }

                generator.writeFieldName("nan_value_count");
                Long nanValueCount = columnMetrics.get(3, Long.class);
                if (nanValueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(nanValueCount);
                }

                generator.writeFieldName("lower_bound");
                SingleValueParser.toJson(field.type(), columnMetrics.get(4, Object.class), generator);

                generator.writeFieldName("upper_bound");
                SingleValueParser.toJson(field.type(), columnMetrics.get(5, Object.class), generator);

                generator.writeEndObject();
            }

            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("JSON conversion failed for: " + readableMetrics, e);
        }
    }

    static Map<Integer, Type> getIcebergIdToTypeMapping(Schema schema)
    {
        ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            populateIcebergIdToTypeMapping(field, icebergIdToTypeMapping);
        }
        return icebergIdToTypeMapping.buildOrThrow();
    }

    private static void populateIcebergIdToTypeMapping(Types.NestedField field, ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping)
    {
        Type type = field.type();
        icebergIdToTypeMapping.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> populateIcebergIdToTypeMapping(child, icebergIdToTypeMapping));
        }
    }
}
