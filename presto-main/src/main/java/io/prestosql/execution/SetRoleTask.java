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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SetRole;
import io.prestosql.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createCatalogName;
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
    public ListenableFuture<?> execute(SetRole statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        String catalog = createCatalogName(session, statement);
        if (statement.getType() == SetRole.Type.ROLE) {
            accessControl.checkCanSetRole(
                    session.getRequiredTransactionId(),
                    session.getIdentity(),
                    statement.getRole().map(c -> c.getValue().toLowerCase(ENGLISH)).get(),
                    catalog);
        }
        SelectedRole.Type type;
        switch (statement.getType()) {
            case ROLE:
                type = SelectedRole.Type.ROLE;
                break;
            case ALL:
                type = SelectedRole.Type.ALL;
                break;
            case NONE:
                type = SelectedRole.Type.NONE;
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + statement.getType());
        }
        stateMachine.addSetRole(catalog, new SelectedRole(type, statement.getRole().map(c -> c.getValue().toLowerCase(ENGLISH))));
        return immediateFuture(null);
    }
}
