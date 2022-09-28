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
package io.trino.connector.system.jdbc;

import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static io.trino.connector.system.jdbc.FilterUtil.emptyOrEquals;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataListing.listTables;
import static io.trino.metadata.MetadataListing.listViews;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
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
            // Non-lowercase predicate will never match a lowercase name (until TODO https://github.com/trinodb/trino/issues/17)
            return table.build().cursor();
        }

        for (String catalog : listCatalogNames(session, metadata, accessControl, catalogFilter)) {
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
