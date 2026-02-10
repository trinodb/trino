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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance;

public abstract class BaseSystemTable
        implements SystemTable
{
    private final Table icebergTable;
    private final ConnectorTableMetadata tableMetadata;
    private final MetadataTableType metadataTableType;
    private final ExecutorService executor;

    BaseSystemTable(Table icebergTable, ConnectorTableMetadata tableMetadata, MetadataTableType metadataTableType, ExecutorService executor)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
        this.metadataTableType = requireNonNull(metadataTableType, "metadataTableType is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        TableScan tableScan = createMetadataTableInstance(icebergTable, metadataTableType).newScan().planWith(executor);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        Map<String, Integer> columnNameToPosition = mapWithIndex(tableScan.schema().columns().stream(),
                (column, position) -> immutableEntry(column.name(), Long.valueOf(position).intValue()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        List<Type> types = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());

        return new IcebergSystemTablePageSource(
                types,
                timeZoneKey,
                this::addRow,
                columnNameToPosition,
                tableScan);
    }

    protected abstract void addRow(IcebergSystemTablePageSource pageSource, Row row, TimeZoneKey timeZoneKey);

    public record Row(StructLike structLike, Map<String, Integer> columnNameToPositionInSchema)
    {
        public Row
        {
            requireNonNull(structLike, "structLike is null");
            columnNameToPositionInSchema = ImmutableMap.copyOf(columnNameToPositionInSchema);
        }

        public <T> T get(String columnName, Class<T> javaClass)
        {
            return structLike.get(columnNameToPositionInSchema.get(columnName), javaClass);
        }
    }
}
