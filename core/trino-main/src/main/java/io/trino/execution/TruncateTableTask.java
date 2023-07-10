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
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.TruncateTable;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class TruncateTableTask
        implements DataDefinitionTask<TruncateTable>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public TruncateTableTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "TRUNCATE TABLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            TruncateTable statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName());

        if (metadata.isMaterializedView(session, tableName)) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot truncate a materialized view");
        }

        if (metadata.isView(session, tableName)) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot truncate a view");
        }

        TableHandle tableHandle = metadata.getTableHandle(session, tableName)
                .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName));

        accessControl.checkCanTruncateTable(session.toSecurityContext(), tableName);

        metadata.truncateTable(session, tableHandle);

        return immediateFuture(null);
    }
}
