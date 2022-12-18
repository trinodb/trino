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
package io.trino.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.Expression;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.checkRoleExists;
import static io.trino.metadata.MetadataUtil.createCatalogSchemaName;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class CreateSchemaTask
        implements DataDefinitionTask<CreateSchema>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final SchemaPropertyManager schemaPropertyManager;

    @Inject
    public CreateSchemaTask(PlannerContext plannerContext, AccessControl accessControl, SchemaPropertyManager schemaPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.schemaPropertyManager = requireNonNull(schemaPropertyManager, "schemaPropertyManager is null");
    }

    @Override
    public String getName()
    {
        return "CREATE SCHEMA";
    }

    @Override
    public ListenableFuture<Void> execute(
            CreateSchema statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        return internalExecute(statement, plannerContext, accessControl, schemaPropertyManager, stateMachine.getSession(), parameters);
    }

    @VisibleForTesting
    static ListenableFuture<Void> internalExecute(
            CreateSchema statement,
            PlannerContext plannerContext,
            AccessControl accessControl,
            SchemaPropertyManager schemaPropertyManager,
            Session session,
            List<Expression> parameters)
    {
        CatalogSchemaName schema = createCatalogSchemaName(session, statement, Optional.of(statement.getSchemaName()));

        String catalogName = schema.getCatalogName();
        CatalogHandle catalogHandle = getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, catalogName);

        Map<String, Object> properties = schemaPropertyManager.getProperties(
                catalogName,
                catalogHandle,
                statement.getProperties(),
                session,
                plannerContext,
                accessControl,
                bindParameters(statement, parameters),
                true);

        Set<String> specifiedPropertyKeys = statement.getProperties().stream()
                // property names are case-insensitive and normalized to lower case
                .map(property -> property.getName().getValue().toLowerCase(ENGLISH))
                .collect(toImmutableSet());
        Map<String, Object> explicitlySetProperties = properties.keySet().stream()
                .peek(key -> verify(key.equals(key.toLowerCase(ENGLISH)), "Property name '%s' not in lower-case", key))
                .filter(specifiedPropertyKeys::contains)
                .collect(toImmutableMap(Function.identity(), properties::get));
        accessControl.checkCanCreateSchema(session.toSecurityContext(), schema, explicitlySetProperties);

        if (plannerContext.getMetadata().schemaExists(session, schema)) {
            if (!statement.isNotExists()) {
                throw semanticException(SCHEMA_ALREADY_EXISTS, statement, "Schema '%s' already exists", schema);
            }
            return immediateVoidFuture();
        }

        TrinoPrincipal principal = getCreatePrincipal(statement, session, plannerContext.getMetadata(), catalogName);
        try {
            plannerContext.getMetadata().createSchema(session, schema, properties, principal);
        }
        catch (TrinoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || !statement.isNotExists()) {
                throw e;
            }
        }

        return immediateVoidFuture();
    }

    private static TrinoPrincipal getCreatePrincipal(CreateSchema statement, Session session, Metadata metadata, String catalog)
    {
        if (statement.getPrincipal().isEmpty()) {
            return new TrinoPrincipal(PrincipalType.USER, session.getUser());
        }

        TrinoPrincipal principal = createPrincipal(statement.getPrincipal().get());
        checkRoleExists(session, statement, metadata, principal, Optional.of(catalog));
        return principal;
    }
}
