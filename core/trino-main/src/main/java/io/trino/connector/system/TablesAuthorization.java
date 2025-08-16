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
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TableAuthorization;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listAllAvailableSchemas;
import static io.trino.metadata.MetadataListing.listTables;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class TablesAuthorization
        implements SystemTable
{
    public static final SchemaTableName TABLES_AUTHORIZATION_NAME = new SchemaTableName("metadata", "tables_authorization");

    public static final ConnectorTableMetadata TABLES_AUTHORIZATION = tableMetadataBuilder(TABLES_AUTHORIZATION_NAME)
            .column("catalog", createUnboundedVarcharType())
            .column("schema", createUnboundedVarcharType())
            .column("name", createUnboundedVarcharType())
            .column("authorization_type", createUnboundedVarcharType())
            .column("authorization", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public TablesAuthorization(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TABLES_AUTHORIZATION;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(TABLES_AUTHORIZATION);
        for (CatalogTableAuthorization catalogTableAuthorization : getTablesAuthorization(session, constraint)) {
            SchemaTableName schemaTableName = catalogTableAuthorization.tableAuthorization().schemaTableName();
            TrinoPrincipal trinoPrincipal = catalogTableAuthorization.tableAuthorization.trinoPrincipal();
            table.addRow(
                    catalogTableAuthorization.catalog(),
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    trinoPrincipal.getType().toString(),
                    trinoPrincipal.getName());
        }
        return table.build().cursor();
    }

    private List<CatalogTableAuthorization> getTablesAuthorization(Session session, TupleDomain<Integer> constraint)
    {
        try {
            return doGetTablesAuthorization(session, constraint);
        }
        catch (RuntimeException exception) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Error access tables_authorization metadata table",
                    exception);
        }
    }

    private List<CatalogTableAuthorization> doGetTablesAuthorization(
            Session session,
            TupleDomain<Integer> constraint)
    {
        Set<CatalogSchemaName> availableSchemas = listAllAvailableSchemas(session,
                metadata,
                accessControl,
                constraint.getDomain(0, VARCHAR),
                constraint.getDomain(1, VARCHAR));
        Optional<String> tableName = tryGetSingleVarcharValue(constraint.getDomain(2, VARCHAR));
        ImmutableList.Builder<CatalogTableAuthorization> result = ImmutableList.builder();
        availableSchemas.forEach(catalogSchemaName -> {
            QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogSchemaName.getCatalogName(), Optional.of(catalogSchemaName.getSchemaName()), tableName);
            Set<SchemaTableName> accessibleNames = listTables(session, metadata, accessControl, prefix);
            Set<TableAuthorization> allTablesAuthorization = metadata.getTablesAuthorizationInfo(session, prefix);
            allTablesAuthorization.stream()
                    .filter(tableAuthorization -> accessibleNames.contains(tableAuthorization.schemaTableName()))
                    .map(tableAuthorization -> new CatalogTableAuthorization(catalogSchemaName.getCatalogName(), tableAuthorization))
                    .forEach(result::add);
        });
        return result.build();
    }

    private record CatalogTableAuthorization(String catalog, TableAuthorization tableAuthorization)
    {
        public CatalogTableAuthorization
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(tableAuthorization, "tableAuthorization is null");
        }
    }
}
