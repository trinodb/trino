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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.iceberg.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class PropertiesTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private static final Set<String> RESERVED_PROPERTIES =
            ImmutableSet.of(
                    "provider",
                    "format",
                    "current-snapshot-id",
                    "location",
                    "format-version");

    public PropertiesTable(SchemaTableName tableName, Table icebergTable)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        this.tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("key", VARCHAR))
                        .add(new ColumnMetadata("value", VARCHAR))
                        .build());
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
        return new FixedPageSource(buildPages(tableMetadata, icebergTable));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, Table icebergTable)
    {
        ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);
        String currentSnapshotId =
                icebergTable.currentSnapshot() != null
                        ? String.valueOf(icebergTable.currentSnapshot().snapshotId())
                        : "none";
        //TableOperations ops = ((BaseTable) icebergTable).operations();
        String fileFormat = icebergTable.properties().getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        propsBuilder.put("format", "iceberg/" + fileFormat);
        propsBuilder.put("provider", "iceberg");
        propsBuilder.put("current-snapshot-id", currentSnapshotId);
        propsBuilder.put("location", icebergTable.location());
        if (icebergTable instanceof BaseTable) {
            TableOperations ops = ((BaseTable) icebergTable).operations();
            propsBuilder.put("format-version", String.valueOf(ops.current().formatVersion()));
        }
        icebergTable.properties().entrySet().stream()
                .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
                .forEach(propsBuilder::put);
        Map<String, String> properties = propsBuilder.buildOrThrow();
        properties.entrySet().forEach(prop -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(prop.getKey());
            pagesBuilder.appendVarchar(prop.getValue());
            pagesBuilder.endRow();
        });
        return pagesBuilder.build();
    }
}
