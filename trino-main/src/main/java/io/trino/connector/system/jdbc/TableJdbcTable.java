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
package io.prestosql.connector.system.jdbc;

import io.prestosql.FullConnectorSession;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.InMemoryRecordSet.Builder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static io.prestosql.connector.system.jdbc.FilterUtil.emptyOrEquals;
import static io.prestosql.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.prestosql.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.prestosql.metadata.MetadataListing.listCatalogs;
import static io.prestosql.metadata.MetadataListing.listTables;
import static io.prestosql.metadata.MetadataListing.listViews;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TableJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "tables");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_cat", createUnboundedVarcharType())
            .column("table_schem", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("table_type", createUnboundedVarcharType())
            .column("remarks", createUnboundedVarcharType())
            .column("type_cat", createUnboundedVarcharType())
            .column("type_schem", createUnboundedVarcharType())
            .column("type_name", createUnboundedVarcharType())
            .column("self_referencing_col_name", createUnboundedVarcharType())
            .column("ref_generation", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public TableJdbcTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        Optional<String> catalogFilter = tryGetSingleVarcharValue(constraint, 0);
        Optional<String> schemaFilter = tryGetSingleVarcharValue(constraint, 1);
        Optional<String> tableFilter = tryGetSingleVarcharValue(constraint, 2);
        Optional<String> typeFilter = tryGetSingleVarcharValue(constraint, 3);

        boolean includeTables = emptyOrEquals(typeFilter, "TABLE");
        boolean includeViews = emptyOrEquals(typeFilter, "VIEW");
        Builder table = InMemoryRecordSet.builder(METADATA);

        if (!includeTables && !includeViews) {
            return table.build().cursor();
        }

        if (isNonLowercase(schemaFilter) || isNonLowercase(tableFilter)) {
            // Non-lowercase predicate will never match a lowercase name (until TODO https://github.com/prestosql/presto/issues/17)
            return table.build().cursor();
        }

        for (String catalog : listCatalogs(session, metadata, accessControl, catalogFilter).keySet()) {
            QualifiedTablePrefix prefix = tablePrefix(catalog, schemaFilter, tableFilter);

            Set<SchemaTableName> views = listViews(session, metadata, accessControl, prefix);
            for (SchemaTableName name : listTables(session, metadata, accessControl, prefix)) {
                boolean isView = views.contains(name);
                if ((includeTables && !isView) || (includeViews && isView)) {
                    table.addRow(tableRow(catalog, name, isView ? "VIEW" : "TABLE"));
                }
            }
        }
        return table.build().cursor();
    }

    private static boolean isNonLowercase(Optional<String> filter)
    {
        return filter.filter(value -> !value.equals(value.toLowerCase(ENGLISH))).isPresent();
    }

    private static Object[] tableRow(String catalog, SchemaTableName name, String type)
    {
        return new Object[] {catalog, name.getSchemaName(), name.getTableName(), type,
                null, null, null, null, null, null};
    }
}
