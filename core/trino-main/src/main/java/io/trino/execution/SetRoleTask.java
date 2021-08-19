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
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.security.SelectedRole;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetRole;
import io.trino.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.getSessionCatalog;
import static java.util.Locale.ENGLISH;

public class SetRoleTask
        implements DataDefinitionTask<SetRole>
{
    @Override
    public String getName()
    {
        return "SET ROLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetRole statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        String catalog = getSessionCatalog(metadata, session, statement);
        if (statement.getType() == SetRole.Type.ROLE) {
            accessControl.checkCanSetRole(
                    SecurityContext.of(session),
                    statement.getRole().map(c -> c.getValue().toLowerCase(ENGLISH)).get(),
                    catalog);
        }
        SelectedRole.Type type = toSelectedRoleType(statement.getType());
        stateMachine.addSetRole(catalog, new SelectedRole(type, statement.getRole().map(c -> c.getValue().toLowerCase(ENGLISH))));
        return immediateVoidFuture();
    }

    private static SelectedRole.Type toSelectedRoleType(SetRole.Type statementRoleType)
    {
        switch (statementRoleType) {
            case ROLE:
                return SelectedRole.Type.ROLE;
            case ALL:
                return SelectedRole.Type.ALL;
            case NONE:
                return SelectedRole.Type.NONE;
        }
        throw new IllegalArgumentException("Unsupported type: " + statementRoleType);
    }
}
