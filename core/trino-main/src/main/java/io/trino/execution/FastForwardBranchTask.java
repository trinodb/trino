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
import io.trino.sql.tree.FastForwardBranch;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class FastForwardBranchTask
        implements DataDefinitionTask<FastForwardBranch>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public FastForwardBranchTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "FAST FORWARD BRANCH";
    }

    @Override
    public ListenableFuture<Void> execute(FastForwardBranch statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        QualifiedObjectName table = createQualifiedObjectName(session, statement, statement.geTableName());
        String sourceBranch = statement.getSourceBranchName().getValue();
        String targetBranch = statement.getTargetBranchName().getValue();

        if (metadata.isMaterializedView(session, table)) {
            throw semanticException(NOT_SUPPORTED, statement, "Fast forwarding branch on materialized view is not supported");
        }
        if (metadata.isView(session, table)) {
            throw semanticException(NOT_SUPPORTED, statement, "Fast forwarding branch on view is not supported");
        }
        Optional<TableHandle> tableHandle = metadata.getRedirectionAwareTableHandle(session, table).tableHandle();
        if (tableHandle.isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", table);
        }

        accessControl.checkCanFastForwardBranch(session.toSecurityContext(), table, sourceBranch, targetBranch);

        if (sourceBranch.equals(targetBranch)) {
            return immediateVoidFuture();
        }
        if (!metadata.branchExists(session, table, sourceBranch)) {
            throw semanticException(BRANCH_NOT_FOUND, statement, "Branch '%s' does not exist", sourceBranch);
        }
        if (!metadata.branchExists(session, table, targetBranch)) {
            throw semanticException(BRANCH_NOT_FOUND, statement, "Branch '%s' does not exist", targetBranch);
        }

        metadata.fastForwardBranch(session, tableHandle.get(), sourceBranch, targetBranch);

        return immediateVoidFuture();
    }
}
