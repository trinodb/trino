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
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.ReplaceTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class ReplaceTagTask
        implements DataDefinitionTask<ReplaceTag>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;

    @Inject
    public ReplaceTagTask(PlannerContext plannerContext, AccessControl accessControl)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "REPLACE TAG";
    }

    @Override
    public ListenableFuture<Void> execute(
            ReplaceTag statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTable());
        RedirectionAwareTableHandle redirectionAwareTableHandle = plannerContext.getMetadata().getRedirectionAwareTableHandle(session, tableName);

        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
        }

        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();

        QualifiedObjectName qualifiedTableName = redirectionAwareTableHandle.redirectedTableName().orElse(tableName);
        accessControl.checkCanSetTableProperties(session.toSecurityContext(), qualifiedTableName, Map.of());
        String tagName = statement.getTagName().getValue();

        Optional<Long> snapshotId = statement.getSnapshotId().map(expr -> {
            if (expr instanceof LongLiteral longLiteral) {
                return longLiteral.getParsedValue();
            }
            throw semanticException(io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, statement, "Snapshot ID must be a number");
        });

        Optional<Duration> retention = statement.getRetentionDays().map(expr -> {
            if (expr instanceof LongLiteral longLiteral) {
                return Duration.ofDays(longLiteral.getParsedValue());
            }
            throw semanticException(io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, statement, "Retention days must be a number");
        });

        plannerContext.getMetadata().replaceTag(session, tableHandle, tagName, snapshotId, retention);

        return immediateVoidFuture();
    }
}
