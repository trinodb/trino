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
import io.trino.security.AccessControl;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.checkRoleExists;
import static io.trino.metadata.MetadataUtil.processRoleCommandCatalog;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DropRoleTask
        implements DataDefinitionTask<DropRole>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DropRoleTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DROP ROLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropRole statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        Optional<String> catalog = processRoleCommandCatalog(metadata, session, statement, statement.getCatalog().map(Identifier::getValue));
        String role = statement.getName().getValue().toLowerCase(ENGLISH);
        if (statement.isExists() && !metadata.roleExists(session, role, catalog)) {
            return immediateVoidFuture();
        }
        accessControl.checkCanDropRole(session.toSecurityContext(), role, catalog);
        checkRoleExists(session, statement, metadata, role, catalog);
        metadata.dropRole(session, role, catalog);
        return immediateVoidFuture();
    }
}
