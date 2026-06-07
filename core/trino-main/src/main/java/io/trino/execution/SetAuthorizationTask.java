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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Resolver;
import io.trino.sql.tree.SetAuthorizationStatement;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.checkRoleExists;
import static io.trino.metadata.MetadataUtil.createCatalogSchemaName;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.fillInNameParts;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class SetAuthorizationTask
        implements DataDefinitionTask<SetAuthorizationStatement>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public SetAuthorizationTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET AUTHORIZATION";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetAuthorizationStatement statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        setEntityAuthorization(session, statement);

        return immediateVoidFuture();
    }

    private void setEntityAuthorization(Session session, SetAuthorizationStatement statement)
    {
        // Preprocess SCHEMA, TABLE and VIEW to generate error messages in the order compatible with existing tests
        int size = statement.getSource().getParts().size();
        boolean withCatalog = size > 2;
        switch (statement.getOwnedEntityKind()) {
            case "SCHEMA" -> {
                CatalogSchemaName source = createCatalogSchemaName(session, statement, Optional.of(statement.getSource()), metadata);
                metadata.getResolverManager().setQueryResolver(session, metadata.getResolverManager().getResolver(session, source.getCatalogName()));
                withCatalog = size > 1;
                if (!metadata.schemaExists(session, source)) {
                    throw semanticException(SCHEMA_NOT_FOUND, statement, "Schema '%s' does not exist", source);
                }
            }
            case "TABLE" -> {
                QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getSource(), metadata);
                metadata.getResolverManager().setQueryResolver(session, metadata.getResolverManager().getResolver(session, tableName.catalogName()));
                getRequiredCatalogHandle(metadata, session, statement, tableName.catalogName());
                RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, tableName);
                if (redirection.tableHandle().isEmpty()) {
                    throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
                }
                if (redirection.redirectedTableName().isPresent()) {
                    throw semanticException(NOT_SUPPORTED, statement, "Table %s is redirected to %s and SET TABLE AUTHORIZATION is not supported with table redirections", tableName, redirection.redirectedTableName().get());
                }
            }
            case "VIEW" -> {
                QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getSource(), metadata);
                metadata.getResolverManager().setQueryResolver(session, metadata.getResolverManager().getResolver(session, viewName.catalogName()));
                getRequiredCatalogHandle(metadata, session, statement, viewName.catalogName());
                if (!metadata.isView(session, viewName)) {
                    throw semanticException(TABLE_NOT_FOUND, statement, "View '%s' does not exist", viewName);
                }
            }
            case "MATERIALIZED VIEW" -> {
                QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getSource(), metadata);
                metadata.getResolverManager().setQueryResolver(session, metadata.getResolverManager().getResolver(session, viewName.catalogName()));
                getRequiredCatalogHandle(metadata, session, statement, viewName.catalogName());
                if (!metadata.isMaterializedView(session, viewName)) {
                    throw semanticException(TABLE_NOT_FOUND, statement, "Materialized view '%s' does not exist", viewName);
                }
            }
        }
        Resolver resolver = metadata.getResolverManager().getQueryResolver(session, Optional.empty());
        QualifiedName qualifiedName = QualifiedName.of(resolver.getCanonicalizer(), statement.getSource(), withCatalog);
        List<String> name = fillInNameParts(session, statement, statement.getOwnedEntityKind(), qualifiedName.getParts());
        TrinoPrincipal principal = createPrincipal(statement.getPrincipal());
        Optional<String> maybeCatalogName = name.size() > 1 ? Optional.of(name.get(0)) : Optional.empty();
        checkRoleExists(session, statement, metadata, principal, maybeCatalogName.filter(catalog -> metadata.isCatalogManagedSecurity(session, catalog)));
        EntityKindAndName entityKindAndName = new EntityKindAndName(statement.getOwnedEntityKind(), name);
        accessControl.checkCanSetEntityAuthorization(session.toSecurityContext(), entityKindAndName, principal);
        metadata.setEntityAuthorization(session, entityKindAndName, principal);
    }
}
