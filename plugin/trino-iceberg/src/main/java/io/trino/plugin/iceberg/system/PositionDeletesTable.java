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
import io.trino.plugin.iceberg.system.positiondeletes.PositionDeletesTableSplitSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class PositionDeletesTable
        implements SystemTable
{
    public static final String FILE_PATH = "file_path";
    public static final String POS = "pos";
    public static final String PARTITION = "partition";
    public static final String SPEC_ID = "spec_id";
    public static final String DELETE_FILE_PATH = "delete_file_path";

    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Optional<IcebergPartitionColumn> partitionColumnType;

    public PositionDeletesTable(TypeManager typeManager, SchemaTableName tableName, Table table)
    {
        this.icebergTable = requireNonNull(table, "table is null");
        List<PartitionField> partitionFields = getAllPartitionFields(table);
        this.partitionColumnType = getPartitionColumnType(typeManager, partitionFields, table.schema());
        this.tableMetadata = createConnectorTableMetadata(
                requireNonNull(typeManager, "typeManager is null"),
                requireNonNull(tableName, "tableName is null"),
                table,
                partitionFields);
    }

    private static ConnectorTableMetadata createConnectorTableMetadata(
            TypeManager typeManager,
            SchemaTableName tableName,
            Table table,
            List<PartitionField> partitionFields)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.add(new ColumnMetadata(FILE_PATH, VARCHAR));
        columns.add(new ColumnMetadata(POS, BIGINT));
        if (!partitionFields.isEmpty()) {
            RowType partitionType = getPartitionColumnType(typeManager, partitionFields, table.schema())
                    .map(IcebergPartitionColumn::rowType)
                    .orElseThrow();
            columns.add(new ColumnMetadata(PARTITION, partitionType));
        }
        columns.add(new ColumnMetadata(SPEC_ID, INTEGER));
        columns.add(new ColumnMetadata(DELETE_FILE_PATH, VARCHAR));
        return new ConnectorTableMetadata(tableName, columns.build());
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
        return Optional.of(new PositionDeletesTableSplitSource(
                icebergTable,
                SchemaParser.toJson(icebergTable.schema()),
                icebergTable.specs().entrySet().stream().collect(toImmutableMap(
                        Map.Entry::getKey,
                        partitionSpec -> PartitionSpecParser.toJson(partitionSpec.getValue()))),
                partitionColumnType));
    }

    @Override
    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session)
    {
        return Optional.of(IcebergTableCredentials.forFileIO(icebergTable.io()));
    }
}
