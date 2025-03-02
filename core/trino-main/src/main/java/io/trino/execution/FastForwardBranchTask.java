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
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FastForwardBranch;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
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

        QualifiedObjectName table = createQualifiedObjectName(session, statement, statement.tableName());
        String from = statement.from().getValue();
        String to = statement.to().getValue();

        // TODO Fix this access control
        accessControl.canCanDropBranch(session.toSecurityContext(), table, from);

        Optional<TableHandle> tableHandle = metadata.getRedirectionAwareTableHandle(session, table).tableHandle();
        if (tableHandle.isEmpty()) {
            return immediateVoidFuture();
        }

        Collection<String> branches = metadata.listBranches(session, tableHandle.get());
        if (!branches.contains(from)) {
            throw new TrinoException(BRANCH_NOT_FOUND, "Branch '%s' does not exit".formatted(from));
        }
        if (!branches.contains(to)) {
            throw new TrinoException(BRANCH_NOT_FOUND, "Branch '%s' does not exit".formatted(to));
        }

        metadata.fastForwardBranch(session, tableHandle.get(), from, to);

        return immediateVoidFuture();
    }
}
