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
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.RenameMaterializedView;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public class RenameMaterializedViewTask
        implements DataDefinitionTask<RenameMaterializedView>
{
    @Override
    public String getName()
    {
        return "RENAME MATERIALIZED VIEW";
    }

    @Override
    public ListenableFuture<Void> execute(
            RenameMaterializedView statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName materializedViewName = createQualifiedObjectName(session, statement, statement.getSource());
        Optional<ConnectorMaterializedViewDefinition> materializedView = metadata.getMaterializedView(session, materializedViewName);
        if (materializedView.isEmpty()) {
            Optional<ConnectorViewDefinition> view = metadata.getView(session, materializedViewName);
            if (view.isPresent()) {
                throw semanticException(
                        TABLE_NOT_FOUND,
                        statement,
                        "Materialized View '%s' does not exist, but a view with that name exists. Did you mean ALTER VIEW %s RENAME ...?", materializedViewName, materializedViewName);
            }

            Optional<TableHandle> table = metadata.getTableHandle(session, materializedViewName);
            if (table.isPresent()) {
                throw semanticException(
                        TABLE_NOT_FOUND,
                        statement,
                        "Materialized View '%s' does not exist, but a table with that name exists. Did you mean ALTER TABLE %s RENAME ...?", materializedViewName, materializedViewName);
            }

            if (statement.isExists()) {
                return immediateVoidFuture();
            }
            throw semanticException(TABLE_NOT_FOUND, statement, "Materialized View '%s' does not exist", materializedViewName);
        }

        QualifiedObjectName target = createQualifiedObjectName(session, statement, statement.getTarget());
        if (metadata.getCatalogHandle(session, target.getCatalogName()).isEmpty()) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Target catalog '%s' does not exist", target.getCatalogName());
        }
        if (metadata.getMaterializedView(session, target).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Target materialized view '%s' already exists", target);
        }
        if (metadata.getView(session, target).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Target materialized view '%s' does not exist, but a view with that name exists.", target);
        }
        if (metadata.getTableHandle(session, target).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Target materialized view '%s' does not exist, but a table with that name exists.", target);
        }
        if (!materializedViewName.getCatalogName().equals(target.getCatalogName())) {
            throw semanticException(NOT_SUPPORTED, statement, "Materialized View rename across catalogs is not supported");
        }

        accessControl.checkCanRenameMaterializedView(session.toSecurityContext(), materializedViewName, target);

        metadata.renameMaterializedView(session, materializedViewName, target);

        return immediateVoidFuture();
    }
}
