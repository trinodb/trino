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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.DropRedirected;
import io.trino.spi.connector.DropResult;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.Expression;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.METADATA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class DropTableTask
        implements DataDefinitionTask<DropTable>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DropTableTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DROP TABLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropTable statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName originalTableName = createQualifiedObjectName(session, statement, statement.getTableName());
        if (metadata.isMaterializedView(session, originalTableName)) {
            throw semanticException(
                    GENERIC_USER_ERROR,
                    statement,
                    "Table '%s' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW %s?", originalTableName, originalTableName);
        }

        if (metadata.isView(session, originalTableName)) {
            throw semanticException(
                    GENERIC_USER_ERROR,
                    statement,
                    "Table '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", originalTableName, originalTableName);
        }

        Optional<RedirectionAwareTableHandle> redirectionAwareTableHandle = Optional.empty();
        try {
            redirectionAwareTableHandle = Optional.of(metadata.getRedirectionAwareTableHandle(session, originalTableName));
            if (redirectionAwareTableHandle.orElseThrow().getTableHandle().isEmpty()) {
                if (!statement.isExists()) {
                    throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", originalTableName);
                }
                return immediateVoidFuture();
            }
        }
        catch (TrinoException e) {
            if (!e.getErrorCode().equals(METADATA_NOT_FOUND.toErrorCode())) {
                throw e;
            }
            // Ignore Exception when metadata not found and force table drop.
        }

        QualifiedObjectName tableName = originalTableName;
        if (redirectionAwareTableHandle.isPresent()) {
            tableName = redirectionAwareTableHandle.get().getRedirectedTableName().orElse(originalTableName);
        }

        DropResult dropResult = dropTable(session, tableName);

        if (dropResult.isRedirected()) {
            // When getRedirectionAwareTableHandle fails to load the table due to metadata missing, and table is redirected.
            // we will get the redirected target name from dropResult.
            CatalogSchemaTableName catalogSchemaTableName = ((DropRedirected) dropResult).getRedirectedTarget();
            tableName = convertFromSchemaTableName(catalogSchemaTableName.getCatalogName())
                    .apply(catalogSchemaTableName.getSchemaTableName());
            dropTable(session, tableName);
        }

        return immediateVoidFuture();
    }

    private DropResult dropTable(Session session, QualifiedObjectName tableName)
    {
        accessControl.checkCanDropTable(session.toSecurityContext(), tableName);
        return metadata.dropTable(session, tableName);
    }
}
