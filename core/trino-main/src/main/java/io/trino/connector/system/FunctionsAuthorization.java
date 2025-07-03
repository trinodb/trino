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
import io.trino.metadata.QualifiedObjectPrefix;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.FunctionAuthorization;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listAllAvailableSchemas;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class FunctionsAuthorization
        implements SystemTable
{
    public static final SchemaTableName FUNCTIONS_AUTHORIZATION_NAME = new SchemaTableName("metadata", "functions_authorization");

    public static final ConnectorTableMetadata FUNCTIONS_AUTHORIZATION = tableMetadataBuilder(FUNCTIONS_AUTHORIZATION_NAME)
            .column("catalog", createUnboundedVarcharType())
            .column("schema", createUnboundedVarcharType())
            .column("name", createUnboundedVarcharType())
            .column("authorization_type", createUnboundedVarcharType())
            .column("authorization", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public FunctionsAuthorization(Metadata metadata, AccessControl accessControl)
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
        return FUNCTIONS_AUTHORIZATION;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(FUNCTIONS_AUTHORIZATION);
        for (CatalogFunctionAuthorization functionAuthorization : getFunctionsAuthorization(session, constraint)) {
            SchemaFunctionName schemaFunctionName = functionAuthorization.functionAuthorization().schemaFunctionName();
            TrinoPrincipal trinoPrincipal = functionAuthorization.functionAuthorization().trinoPrincipal();
            table.addRow(
                    functionAuthorization.catalog(),
                    schemaFunctionName.getSchemaName(),
                    schemaFunctionName.getFunctionName(),
                    trinoPrincipal.getType().toString(),
                    trinoPrincipal.getName());
        }
        return table.build().cursor();
    }

    private List<CatalogFunctionAuthorization> getFunctionsAuthorization(Session session, TupleDomain<Integer> constraint)
    {
        try {
            return doGetFunctionsAuthorization(session, constraint);
        }
        catch (RuntimeException exception) {
            ErrorCodeSupplier result = GENERIC_INTERNAL_ERROR;
            if (exception instanceof TrinoException trinoException) {
                result = trinoException::getErrorCode;
            }
            throw new TrinoException(
                    result,
                    "Error access functions_authorization metadata table",
                    exception);
        }
    }

    private List<CatalogFunctionAuthorization> doGetFunctionsAuthorization(Session session, TupleDomain<Integer> constraint)
    {
        Domain catalogDomain = constraint.getDomain(0, VARCHAR);
        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        Set<CatalogSchemaName> availableSchemas = listAllAvailableSchemas(
                session,
                metadata,
                accessControl,
                catalogDomain,
                schemaDomain);

        Optional<String> functionName = tryGetSingleVarcharValue(constraint.getDomain(2, VARCHAR));
        ImmutableList.Builder<CatalogFunctionAuthorization> result = ImmutableList.builder();
        availableSchemas.forEach(catalogSchemaName -> {
            Set<FunctionAuthorization> allFunctionsAuthorization = metadata.getFunctionsAuthorizationInfo(session, new QualifiedObjectPrefix(catalogSchemaName.getCatalogName(), Optional.of(catalogSchemaName.getSchemaName()), functionName));
            Set<SchemaFunctionName> filteredFunctions = accessControl.filterFunctions(
                    session.toSecurityContext(),
                    catalogSchemaName.getCatalogName(),
                    allFunctionsAuthorization.stream().map(FunctionAuthorization::schemaFunctionName).collect(toImmutableSet()));

            allFunctionsAuthorization.stream()
                    .filter(functionAuthorization -> filteredFunctions.contains(functionAuthorization.schemaFunctionName()))
                    .map(functionAuthorization -> new CatalogFunctionAuthorization(catalogSchemaName.getCatalogName(), functionAuthorization))
                    .forEach(result::add);
        });
        return result.build();
    }

    private record CatalogFunctionAuthorization(String catalog, FunctionAuthorization functionAuthorization)
    {
        public CatalogFunctionAuthorization
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(functionAuthorization, "functionAuthorization is null");
        }
    }
}
