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
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.Expression;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public class DropTableTask
        implements DataDefinitionTask<DropTable>
{
    @Override
    public String getName()
    {
        return "DROP TABLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropTable statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName());

        Optional<ConnectorMaterializedViewDefinition> materializedView = metadata.getMaterializedView(session, tableName);
        if (materializedView.isPresent()) {
            if (!statement.isExists()) {
                throw semanticException(
                        TABLE_NOT_FOUND,
                        statement,
                        "Table '%s' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW %s?", tableName, tableName);
            }
            return immediateVoidFuture();
        }

        Optional<ConnectorViewDefinition> view = metadata.getView(session, tableName);
        if (view.isPresent()) {
            if (!statement.isExists()) {
                throw semanticException(
                        TABLE_NOT_FOUND,
                        statement,
                        "Table '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", tableName, tableName);
            }
            return immediateVoidFuture();
        }

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (tableHandle.isEmpty()) {
            if (!statement.isExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
            }
            return immediateVoidFuture();
        }

        accessControl.checkCanDropTable(session.toSecurityContext(), tableName);

        metadata.dropTable(session, tableHandle.get());

        return immediateVoidFuture();
    }
}
