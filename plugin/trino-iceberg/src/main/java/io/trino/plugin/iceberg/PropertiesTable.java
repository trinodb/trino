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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.IcebergUtil.createColumnHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class PropertiesTable
        implements IcebergSystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Map<String, ColumnHandle> columnHandles;
    private final Table icebergTable;

    public PropertiesTable(SchemaTableName tableName, Table icebergTable)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        List<IcebergColumnHandle> columnHandlesList = ImmutableList.<IcebergColumnHandle>builder()
                .add(createColumnHandle(primitiveColumnIdentity(1, "key"), VARCHAR))
                .add(createColumnHandle(primitiveColumnIdentity(2, "value"), VARCHAR))
                .build();
        columnHandles = columnHandlesList.stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, Function.identity()));
        this.tableMetadata = new ConnectorTableMetadata(
                requireNonNull(tableName, "tableName is null"),
                columnHandlesList.stream()
                        .map(icebergColumnHandle -> new ColumnMetadata(icebergColumnHandle.getName(), icebergColumnHandle.getType()))
                        .collect(Collectors.toUnmodifiableList()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new FixedPageSource(buildPages(tableMetadata, icebergTable));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        icebergTable.properties().entrySet().forEach(prop -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(prop.getKey());
            pagesBuilder.appendVarchar(prop.getValue());
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }
}
