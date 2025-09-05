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
package io.trino.plugin.iceberg.system;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.system.files.FilesTableSplitSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class FilesTable
        implements SystemTable
{
    public static final String CONTENT_COLUMN_NAME = "content";
    public static final String FILE_PATH_COLUMN_NAME = "file_path";
    public static final String FILE_FORMAT_COLUMN_NAME = "file_format";
    public static final String SPEC_ID_COLUMN_NAME = "spec_id";
    public static final String PARTITION_COLUMN_NAME = "partition";
    public static final String RECORD_COUNT_COLUMN_NAME = "record_count";
    public static final String FILE_SIZE_IN_BYTES_COLUMN_NAME = "file_size_in_bytes";
    public static final String COLUMN_SIZES_COLUMN_NAME = "column_sizes";
    public static final String VALUE_COUNTS_COLUMN_NAME = "value_counts";
    public static final String NULL_VALUE_COUNTS_COLUMN_NAME = "null_value_counts";
    public static final String NAN_VALUE_COUNTS_COLUMN_NAME = "nan_value_counts";
    public static final String LOWER_BOUNDS_COLUMN_NAME = "lower_bounds";
    public static final String UPPER_BOUNDS_COLUMN_NAME = "upper_bounds";
    public static final String KEY_METADATA_COLUMN_NAME = "key_metadata";
    public static final String SPLIT_OFFSETS_COLUMN_NAME = "split_offsets";
    public static final String EQUALITY_IDS_COLUMN_NAME = "equality_ids";
    public static final String SORT_ORDER_ID_COLUMN_NAME = "sort_order_id";
    public static final String READABLE_METRICS_COLUMN_NAME = "readable_metrics";

    private static final List<String> COLUMN_NAMES = ImmutableList.of(
            CONTENT_COLUMN_NAME,
            FILE_PATH_COLUMN_NAME,
            FILE_FORMAT_COLUMN_NAME,
            SPEC_ID_COLUMN_NAME,
            PARTITION_COLUMN_NAME,
            RECORD_COUNT_COLUMN_NAME,
            FILE_SIZE_IN_BYTES_COLUMN_NAME,
            COLUMN_SIZES_COLUMN_NAME,
            VALUE_COUNTS_COLUMN_NAME,
            NULL_VALUE_COUNTS_COLUMN_NAME,
            NAN_VALUE_COUNTS_COLUMN_NAME,
            LOWER_BOUNDS_COLUMN_NAME,
            UPPER_BOUNDS_COLUMN_NAME,
            KEY_METADATA_COLUMN_NAME,
            SPLIT_OFFSETS_COLUMN_NAME,
            EQUALITY_IDS_COLUMN_NAME,
            SORT_ORDER_ID_COLUMN_NAME,
            READABLE_METRICS_COLUMN_NAME);

    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;
    private final Optional<Type> partitionColumnType;

    public FilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");

        List<PartitionField> partitionFields = getAllPartitionFields(icebergTable);
        this.partitionColumnType = getPartitionColumnType(typeManager, partitionFields, icebergTable.schema())
                .map(IcebergPartitionColumn::rowType);

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (String columnName : COLUMN_NAMES) {
            if (columnName.equals(PARTITION_COLUMN_NAME)) {
                partitionColumnType.ifPresent(type -> columns.add(new ColumnMetadata(columnName, type)));
            }
            else {
                columns.add(new ColumnMetadata(columnName, getColumnType(columnName, typeManager)));
            }
        }
        this.tableMetadata = new ConnectorTableMetadata(tableName, columns.build());
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
        try (FileIO fileIO = icebergTable.io()) {
            return Optional.of(new FilesTableSplitSource(
                    icebergTable,
                    snapshotId,
                    SchemaParser.toJson(icebergTable.schema()),
                    SchemaParser.toJson(MetadataTableUtils.createMetadataTableInstance(icebergTable, MetadataTableType.FILES).schema()),
                    icebergTable.specs().entrySet().stream().collect(toImmutableMap(
                            Map.Entry::getKey,
                            partitionSpec -> PartitionSpecParser.toJson(partitionSpec.getValue()))),
                    partitionColumnType,
                    fileIO.properties()));
        }
    }

    public static Type getColumnType(String columnName, TypeManager typeManager)
    {
        return switch (columnName) {
            case CONTENT_COLUMN_NAME,
                 SORT_ORDER_ID_COLUMN_NAME,
                 SPEC_ID_COLUMN_NAME -> INTEGER;
            case FILE_PATH_COLUMN_NAME,
                 FILE_FORMAT_COLUMN_NAME -> VARCHAR;
            case RECORD_COUNT_COLUMN_NAME,
                 FILE_SIZE_IN_BYTES_COLUMN_NAME -> BIGINT;
            case COLUMN_SIZES_COLUMN_NAME,
                 NULL_VALUE_COUNTS_COLUMN_NAME,
                 VALUE_COUNTS_COLUMN_NAME,
                 NAN_VALUE_COUNTS_COLUMN_NAME -> typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()));
            case LOWER_BOUNDS_COLUMN_NAME,
                 UPPER_BOUNDS_COLUMN_NAME -> typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()));
            case KEY_METADATA_COLUMN_NAME -> VARBINARY;
            case SPLIT_OFFSETS_COLUMN_NAME -> new ArrayType(BIGINT);
            case EQUALITY_IDS_COLUMN_NAME -> new ArrayType(INTEGER);
            case READABLE_METRICS_COLUMN_NAME -> typeManager.getType(new TypeSignature(JSON));
            default -> throw new IllegalArgumentException("Unexpected value: " + columnName);
        };
    }
}
