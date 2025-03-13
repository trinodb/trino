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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class IcebergTablesSystemTable
        implements SystemTable
{
    public static final SchemaTableName NAME = new SchemaTableName("system", "iceberg_tables");

    public static final ConnectorTableMetadata METADATA = new ConnectorTableMetadata(
            NAME,
            ImmutableList.<ColumnMetadata>builder()
                    .add(new ColumnMetadata("table_schema", VARCHAR))
                    .add(new ColumnMetadata("table_name", VARCHAR))
                    .build());

    private final TrinoCatalogFactory trinoCatalogFactory;

    @Inject
    public IcebergTablesSystemTable(TrinoCatalogFactory trinoCatalogFactory)
    {
        this.trinoCatalogFactory = requireNonNull(trinoCatalogFactory, "trinoCatalogFactory is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession connectorSession,
            TupleDomain<Integer> constraint,
            Set<Integer> requiredColumns,
            ConnectorSplit split,
            ConnectorAccessControl accessControl)
    {
        Builder result = InMemoryRecordSet.builder(METADATA);

        Domain schemaDomain = constraint.getDomain(0, VARCHAR);

        Optional<String> schemaFilter = tryGetSingleVarcharValue(schemaDomain);

        TrinoCatalog catalog = trinoCatalogFactory.create(connectorSession.getIdentity());
        List<SchemaTableName> icebergTables = catalog.listIcebergTables(connectorSession, schemaFilter);
        Set<SchemaTableName> accessibleIcebergTables = accessControl.filterTables(null, ImmutableSet.copyOf(icebergTables));
        for (SchemaTableName table : accessibleIcebergTables) {
            result.addRow(table.getSchemaName(), table.getTableName());
        }
        return result.build().cursor();
    }

    private static <T> Optional<String> tryGetSingleVarcharValue(Domain domain)
    {
        if (!domain.isSingleValue()) {
            return Optional.empty();
        }
        Object value = domain.getSingleValue();
        return Optional.of(((Slice) value).toStringUtf8());
    }
}
