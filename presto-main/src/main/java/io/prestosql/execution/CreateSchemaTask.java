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
package io.prestosql.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.sql.tree.CreateSchema;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createCatalogSchemaName;
import static io.prestosql.metadata.MetadataUtil.createPrincipal;
import static io.prestosql.metadata.MetadataUtil.getSessionCatalog;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.prestosql.sql.NodeUtils.mapFromProperties;
import static io.prestosql.sql.ParameterUtils.parameterExtractor;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

public class CreateSchemaTask
        implements DataDefinitionTask<CreateSchema>
{
    @Override
    public String getName()
    {
        return "CREATE SCHEMA";
    }

    @Override
    public String explain(CreateSchema statement, List<Expression> parameters)
    {
        return "CREATE SCHEMA " + statement.getSchemaName();
    }

    @Override
    public ListenableFuture<?> execute(CreateSchema statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        return internalExecute(statement, metadata, accessControl, stateMachine.getSession(), parameters);
    }

    @VisibleForTesting
    ListenableFuture<?> internalExecute(CreateSchema statement, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters)
    {
        CatalogSchemaName schema = createCatalogSchemaName(session, statement, Optional.of(statement.getSchemaName()));

        // TODO: validate that catalog exists

        accessControl.checkCanCreateSchema(session.toSecurityContext(), schema);

        if (metadata.schemaExists(session, schema)) {
            if (!statement.isNotExists()) {
                throw semanticException(SCHEMA_ALREADY_EXISTS, statement, "Schema '%s' already exists", schema);
            }
            return immediateFuture(null);
        }

        CatalogName catalogName = metadata.getCatalogHandle(session, schema.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + schema.getCatalogName()));

        Map<String, Object> properties = metadata.getSchemaPropertyManager().getProperties(
                catalogName,
                schema.getCatalogName(),
                mapFromProperties(statement.getProperties()),
                session,
                metadata,
                accessControl,
                parameterExtractor(statement, parameters));

        PrestoPrincipal principal = getCreatePrincipal(statement, session, metadata);
        try {
            metadata.createSchema(session, schema, properties, principal);
        }
        catch (PrestoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || !statement.isNotExists()) {
                throw e;
            }
        }

        return immediateFuture(null);
    }

    private PrestoPrincipal getCreatePrincipal(CreateSchema statement, Session session, Metadata metadata)
    {
        if (statement.getPrincipal().isPresent()) {
            PrestoPrincipal principal = createPrincipal(statement.getPrincipal().get());
            String catalog = getSessionCatalog(metadata, session, statement);
            if (principal.getType() == PrincipalType.ROLE
                    && !metadata.listRoles(session, catalog).contains(principal.getName())) {
                throw semanticException(ROLE_NOT_FOUND, statement, "Role '%s' does not exist", principal.getName());
            }
            return principal;
        }
        else {
            return new PrestoPrincipal(PrincipalType.USER, session.getUser());
        }
    }
}
