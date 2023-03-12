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
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.Privilege;
import io.trino.sql.tree.Deny;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GrantOnType;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.PrivilegeUtilities.parseStatementPrivileges;
import static io.trino.metadata.MetadataUtil.createCatalogSchemaName;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class DenyTask
        implements DataDefinitionTask<Deny>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DenyTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DENY";
    }

    @Override
    public ListenableFuture<Void> execute(Deny statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        if (statement.getType().filter(GrantOnType.SCHEMA::equals).isPresent()) {
            executeDenyOnSchema(stateMachine.getSession(), statement, metadata, accessControl);
        }
        else {
            executeDenyOnTable(stateMachine.getSession(), statement, metadata, accessControl);
        }
        return immediateVoidFuture();
    }

    private static void executeDenyOnSchema(Session session, Deny statement, Metadata metadata, AccessControl accessControl)
    {
        CatalogSchemaName schemaName = createCatalogSchemaName(session, statement, Optional.of(statement.getName()));

        if (!metadata.schemaExists(session, schemaName)) {
            throw semanticException(SCHEMA_NOT_FOUND, statement, "Schema '%s' does not exist", schemaName);
        }

        Set<Privilege> privileges = parseStatementPrivileges(statement, statement.getPrivileges());
        for (Privilege privilege : privileges) {
            accessControl.checkCanDenySchemaPrivilege(session.toSecurityContext(), privilege, schemaName, createPrincipal(statement.getGrantee()));
        }

        metadata.denySchemaPrivileges(session, schemaName, privileges, createPrincipal(statement.getGrantee()));
    }

    private static void executeDenyOnTable(Session session, Deny statement, Metadata metadata, AccessControl accessControl)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, tableName);
        if (redirection.getTableHandle().isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
        }
        if (redirection.getRedirectedTableName().isPresent()) {
            throw semanticException(NOT_SUPPORTED, statement, "Table %s is redirected to %s and DENY is not supported with table redirections", tableName, redirection.getRedirectedTableName().get());
        }

        Set<Privilege> privileges = parseStatementPrivileges(statement, statement.getPrivileges());

        for (Privilege privilege : privileges) {
            accessControl.checkCanDenyTablePrivilege(session.toSecurityContext(), privilege, tableName, createPrincipal(statement.getGrantee()));
        }

        metadata.denyTablePrivileges(session, tableName, privileges, createPrincipal(statement.getGrantee()));
    }
}
