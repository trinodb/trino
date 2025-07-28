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
import org.apache.iceberg.SortOrder;

import java.util.List;
import java.util.Set;

import static io.trino.plugin.iceberg.SortFieldUtils.toSortFields;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableUtil.formatVersion;

public class PropertiesTable
        implements SystemTable
{
    private static final Set<String> RESERVED_PROPERTIES = ImmutableSet.<String>builder()
            .add("provider")
            .add("format")
            .add("current-snapshot-id")
            .add("location")
            .add("format-version")
            .build();

    private final ConnectorTableMetadata tableMetadata;
    private final BaseTable icebergTable;

    public PropertiesTable(SchemaTableName tableName, BaseTable icebergTable)
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

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, BaseTable icebergTable)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        String currentSnapshotId = icebergTable.currentSnapshot() != null ? String.valueOf(icebergTable.currentSnapshot().snapshotId()) : "none";
        String fileFormat = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        properties.put("format", "iceberg/" + fileFormat);
        properties.put("provider", "iceberg");
        properties.put("current-snapshot-id", currentSnapshotId);
        properties.put("location", icebergTable.location());
        properties.put("format-version", String.valueOf(formatVersion(icebergTable)));
        // TODO: Support sort column transforms (https://github.com/trinodb/trino/issues/15088)
        SortOrder sortOrder = icebergTable.sortOrder();
        if (!sortOrder.isUnsorted() && sortOrder.fields().stream().allMatch(sortField -> sortField.transform().isIdentity())) {
            List<String> sortColumnNames = toSortFields(sortOrder);
            properties.put("sort-order", String.join(", ", sortColumnNames));
        }
        icebergTable.properties().entrySet().stream()
                .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
                .forEach(properties::put);

        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);
        properties.buildOrThrow().entrySet().forEach(prop -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(prop.getKey());
            pagesBuilder.appendVarchar(prop.getValue());
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }
}
