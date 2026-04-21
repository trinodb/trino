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
import io.trino.metadata.QualifiedSchemaPrefix;
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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.SchemaAuthorization;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listAllAvailableSchemas;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

public class SchemasAuthorization
        implements SystemTable
{
    public static final SchemaTableName SCHEMAS_AUTHORIZATION_NAME = new SchemaTableName("metadata", "schemas_authorization");

    public static final ConnectorTableMetadata SCHEMAS_AUTHORIZATION = tableMetadataBuilder(SCHEMAS_AUTHORIZATION_NAME)
            .column("catalog", createUnboundedVarcharType())
            .column("schema", createUnboundedVarcharType())
            .column("authorization_type", createUnboundedVarcharType())
            .column("authorization", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public SchemasAuthorization(Metadata metadata, AccessControl accessControl)
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
        return SCHEMAS_AUTHORIZATION;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(SCHEMAS_AUTHORIZATION);
        for (CatalogSchemaAuthorization catalogSchemaAuthorization : getSchemasAuthorization(session, constraint)) {
            TrinoPrincipal trinoPrincipal = catalogSchemaAuthorization.schemaAuthorization().trinoPrincipal();
            table.addRow(
                    catalogSchemaAuthorization.catalog(),
                    catalogSchemaAuthorization.schemaAuthorization().schemaName(),
                    trinoPrincipal.getType().toString(),
                    trinoPrincipal.getName());
        }
        return table.build().cursor();
    }

    private List<CatalogSchemaAuthorization> getSchemasAuthorization(Session session, TupleDomain<Integer> constraint)
    {
        try {
            return doGetSchemasAuthorization(session, constraint);
        }
        catch (RuntimeException exception) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Error access schemas_authorizations metadata table",
                    exception);
        }
    }

    private List<CatalogSchemaAuthorization> doGetSchemasAuthorization(Session session, TupleDomain<Integer> constraint)
    {
        Domain catalogDomain = constraint.getDomain(0, VARCHAR);
        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        Set<CatalogSchemaName> availableSchemas = listAllAvailableSchemas(
                session,
                metadata,
                accessControl,
                catalogDomain,
                schemaDomain);
        Optional<String> schemaName = tryGetSingleVarcharValue(schemaDomain);
        Map<String, Set<CatalogSchemaName>> groupedByCatalog = availableSchemas.stream()
                .collect(groupingBy(CatalogSchemaName::getCatalogName, Collectors.toSet()));
        ImmutableList.Builder<CatalogSchemaAuthorization> result = ImmutableList.builder();
        for (String catalog : groupedByCatalog.keySet()) {
            Set<SchemaAuthorization> allSchemasAuthorization = metadata.getSchemasAuthorizationInfo(session, new QualifiedSchemaPrefix(catalog, schemaName));
            allSchemasAuthorization.stream()
                    .filter(schemaAuthorization -> groupedByCatalog.get(catalog).contains(new CatalogSchemaName(catalog, schemaAuthorization.schemaName())))
                    .map(schemaAuthorization -> new CatalogSchemaAuthorization(catalog, schemaAuthorization))
                    .forEach(result::add);
        }
        return result.build();
    }

    private record CatalogSchemaAuthorization(String catalog, SchemaAuthorization schemaAuthorization)
    {
        public CatalogSchemaAuthorization
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(schemaAuthorization, "schemaAuthorization is null");
        }
    }
}
