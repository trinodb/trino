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
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.RenameTable;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RenameTableTask
        implements DataDefinitionTask<RenameTable>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public RenameTableTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "RENAME TABLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            RenameTable statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getSource(), metadata);

        if (metadata.isMaterializedView(session, tableName)) {
            throw semanticException(
                    GENERIC_USER_ERROR,
                    statement,
                    "Table '%s' does not exist, but a materialized view with that name exists. Did you mean ALTER MATERIALIZED VIEW %s RENAME TO ...?", tableName, tableName);
        }

        if (metadata.isView(session, tableName)) {
            throw semanticException(
                    GENERIC_USER_ERROR,
                    statement,
                    "Table '%s' does not exist, but a view with that name exists. Did you mean ALTER VIEW %s RENAME TO ...?", tableName, tableName);
        }

        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, tableName);
        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            if (!statement.isExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
            }
            return immediateVoidFuture();
        }

        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();
        QualifiedObjectName source = redirectionAwareTableHandle.redirectedTableName().orElse(tableName);
        QualifiedObjectName target = createTargetQualifiedObjectName(source, statement.getTarget());
        if (metadata.getCatalogHandle(session, target.catalogName()).isEmpty()) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Target catalog '%s' not found", target.catalogName());
        }
        if (metadata.isMaterializedView(session, target)) {
            throw semanticException(GENERIC_USER_ERROR, statement, "Target table '%s' does not exist, but a materialized view with that name exists.", target);
        }
        if (metadata.isView(session, target)) {
            throw semanticException(GENERIC_USER_ERROR, statement, "Target table '%s' does not exist, but a view with that name exists.", target);
        }
        if (metadata.getTableHandle(session, target).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Target table '%s' already exists", target);
        }
        if (!tableHandle.catalogHandle().getCatalogName().toString().equals(target.catalogName())) {
            throw semanticException(NOT_SUPPORTED, statement, "Table rename across catalogs is not supported");
        }
        accessControl.checkCanRenameTable(session.toSecurityContext(), source, target);

        metadata.renameTable(session, tableHandle, source.asCatalogSchemaTableName(), target);

        return immediateVoidFuture();
    }

    private static QualifiedObjectName createTargetQualifiedObjectName(QualifiedObjectName source, QualifiedName target)
    {
        requireNonNull(target, "target is null");
        if (target.getParts().size() > 3) {
            throw new TrinoException(SYNTAX_ERROR, format("Too many dots in table name: %s", target));
        }

        List<String> parts = target.getParts().reversed();
        String objectName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : source.schemaName();
        String catalogName = (parts.size() > 2) ? parts.get(2) : source.catalogName();

        return new QualifiedObjectName(catalogName, schemaName, objectName);
    }
}
