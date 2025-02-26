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
import io.trino.spi.TrinoException;
import io.trino.sql.tree.DropBranch;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class DropBranchTask
        implements DataDefinitionTask<DropBranch>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DropBranchTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DROP BRANCH";
    }

    @Override
    public ListenableFuture<Void> execute(DropBranch statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        QualifiedObjectName table = createQualifiedObjectName(session, statement, statement.tableName());
        String branch = statement.branchName().getValue();

        accessControl.canCanDropBranch(session.toSecurityContext(), table, branch);

        Optional<TableHandle> tableHandle = metadata.getRedirectionAwareTableHandle(session, table).tableHandle();
        if (tableHandle.isEmpty()) {
            return immediateVoidFuture();
        }

        if (!metadata.branchExists(session, tableHandle.get(), branch)) {
            throw new TrinoException(BRANCH_NOT_FOUND, "Branch '%s' does not exit".formatted(branch));
        }

        metadata.dropBranch(session, tableHandle.get(), branch);

        return immediateVoidFuture();
    }
}
