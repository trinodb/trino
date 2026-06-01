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
import io.trino.plugin.iceberg.IcebergTableCredentials;
import io.trino.plugin.iceberg.system.entries.EntriesTableSplitSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;
import static org.apache.iceberg.MetadataTableType.ENTRIES;
import static org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance;

// https://iceberg.apache.org/docs/latest/spark-queries/#all-entries
// https://iceberg.apache.org/docs/latest/spark-queries/#entries
//
// Distributed, split-per-manifest implementation (mirrors FilesTable). Unlike the metadata-table scan used by the
// other BaseSystemTable subclasses, this never calls reachableManifests/planFiles on the coordinator, so $all_entries
// does not materialize O(snapshots^2) ManifestFile objects in one heap. See EntriesTableSplitSource.
public class EntriesTable
        implements SystemTable
{
    public static final String STATUS_COLUMN_NAME = "status";
    public static final String SNAPSHOT_ID_COLUMN_NAME = "snapshot_id";
    public static final String SEQUENCE_NUMBER_COLUMN_NAME = "sequence_number";
    public static final String FILE_SEQUENCE_NUMBER_COLUMN_NAME = "file_sequence_number";
    public static final String DATA_FILE_COLUMN_NAME = "data_file";
    public static final String READABLE_METRICS_COLUMN_NAME = "readable_metrics";

    private final Table icebergTable;
    private final MetadataTableType metadataTableType;
    private final ExecutorService executor;
    private final ConnectorTableMetadata tableMetadata;

    public EntriesTable(TypeManager typeManager, SchemaTableName tableName, Table icebergTable, MetadataTableType metadataTableType, ExecutorService executor)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.metadataTableType = requireNonNull(metadataTableType, "metadataTableType is null");
        this.executor = requireNonNull(executor, "executor is null");
        checkArgument(metadataTableType == ALL_ENTRIES || metadataTableType == ENTRIES, "Unexpected metadata table type: %s", metadataTableType);
        this.tableMetadata = new ConnectorTableMetadata(
                requireNonNull(tableName, "tableName is null"),
                columns(typeManager, icebergTable));
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
    public Optional<ConnectorSplitSource> splitSource(ConnectorSession session, TupleDomain<ColumnHandle> constraint)
    {
        return Optional.of(new EntriesTableSplitSource(
                icebergTable,
                metadataTableType,
                executor,
                SchemaParser.toJson(icebergTable.schema()),
                SchemaParser.toJson(createMetadataTableInstance(icebergTable, metadataTableType).schema()),
                icebergTable.specs().entrySet().stream().collect(toImmutableMap(
                        Map.Entry::getKey,
                        partitionSpec -> PartitionSpecParser.toJson(partitionSpec.getValue())))));
    }

    @Override
    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session)
    {
        return Optional.of(IcebergTableCredentials.forFileIO(icebergTable.io()));
    }

    private static List<ColumnMetadata> columns(TypeManager typeManager, Table icebergTable)
    {
        return ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata(STATUS_COLUMN_NAME, INTEGER))
                .add(new ColumnMetadata(SNAPSHOT_ID_COLUMN_NAME, BIGINT))
                .add(new ColumnMetadata(SEQUENCE_NUMBER_COLUMN_NAME, BIGINT))
                .add(new ColumnMetadata(FILE_SEQUENCE_NUMBER_COLUMN_NAME, BIGINT))
                .add(new ColumnMetadata(DATA_FILE_COLUMN_NAME, dataFileRowType(typeManager, icebergTable.schema(), icebergTable.specs())))
                .add(new ColumnMetadata(READABLE_METRICS_COLUMN_NAME, typeManager.getType(new TypeSignature(JSON))))
                .build();
    }

    /**
     * The {@code data_file} ROW type, shared by the table metadata and the per-manifest page source so both agree on
     * field order and types. Reconstructable on a worker from the (serialized) schema and partition specs.
     */
    public static RowType dataFileRowType(TypeManager typeManager, Schema schema, Map<Integer, PartitionSpec> specsById)
    {
        List<PartitionField> partitionFields = getAllPartitionFields(schema, specsById);
        Optional<IcebergPartitionColumn> partitionColumnType = getPartitionColumnType(typeManager, partitionFields, schema);

        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        fields.add(new RowType.Field(Optional.of("content"), INTEGER));
        fields.add(new RowType.Field(Optional.of("file_path"), VARCHAR));
        fields.add(new RowType.Field(Optional.of("file_format"), VARCHAR));
        fields.add(new RowType.Field(Optional.of("spec_id"), INTEGER));
        partitionColumnType.ifPresent(type -> fields.add(new RowType.Field(Optional.of("partition"), type.rowType())));
        fields.add(new RowType.Field(Optional.of("record_count"), BIGINT));
        fields.add(new RowType.Field(Optional.of("file_size_in_bytes"), BIGINT));
        fields.add(new RowType.Field(Optional.of("column_sizes"), new MapType(INTEGER, BIGINT, typeManager.getTypeOperators())));
        fields.add(new RowType.Field(Optional.of("value_counts"), new MapType(INTEGER, BIGINT, typeManager.getTypeOperators())));
        fields.add(new RowType.Field(Optional.of("null_value_counts"), new MapType(INTEGER, BIGINT, typeManager.getTypeOperators())));
        fields.add(new RowType.Field(Optional.of("nan_value_counts"), new MapType(INTEGER, BIGINT, typeManager.getTypeOperators())));
        fields.add(new RowType.Field(Optional.of("lower_bounds"), new MapType(INTEGER, VARCHAR, typeManager.getTypeOperators())));
        fields.add(new RowType.Field(Optional.of("upper_bounds"), new MapType(INTEGER, VARCHAR, typeManager.getTypeOperators())));
        fields.add(new RowType.Field(Optional.of("key_metadata"), VARBINARY));
        fields.add(new RowType.Field(Optional.of("split_offsets"), new ArrayType(BIGINT)));
        fields.add(new RowType.Field(Optional.of("equality_ids"), new ArrayType(INTEGER)));
        fields.add(new RowType.Field(Optional.of("sort_order_id"), INTEGER));
        return RowType.from(fields.build());
    }
}
