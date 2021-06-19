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
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.Expression;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Locale.ENGLISH;

public class DropColumnTask
        implements DataDefinitionTask<DropColumn>
{
    @Override
    public String getName()
    {
        return "DROP COLUMN";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropColumn statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTable());
        Optional<TableHandle> tableHandleOptional = metadata.getTableHandle(session, tableName);

        if (tableHandleOptional.isEmpty()) {
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
            }
            return immediateVoidFuture();
        }
        TableHandle tableHandle = tableHandleOptional.get();

        String column = statement.getColumn().getValue().toLowerCase(ENGLISH);

        accessControl.checkCanDropColumn(session.toSecurityContext(), tableName);

        ColumnHandle columnHandle = metadata.getColumnHandles(session, tableHandle).get(column);
        if (columnHandle == null) {
            if (!statement.isColumnExists()) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", column);
            }
            return immediateVoidFuture();
        }

        if (metadata.getColumnMetadata(session, tableHandle, columnHandle).isHidden()) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot drop hidden column");
        }

        if (metadata.getTableMetadata(session, tableHandle).getColumns().stream()
                .filter(info -> !info.isHidden()).count() <= 1) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot drop the only column in a table");
        }

        metadata.dropColumn(session, tableHandle, columnHandle);

        return immediateVoidFuture();
    }
}
