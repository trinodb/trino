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
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropRole;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createCatalogName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_ROLE;
import static java.util.Locale.ENGLISH;

public class DropRoleTask
        implements DataDefinitionTask<DropRole>
{
    @Override
    public String getName()
    {
        return "DROP ROLE";
    }

    @Override
    public ListenableFuture<?> execute(DropRole statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        String catalog = createCatalogName(session, statement);
        String role = statement.getName().getValue().toLowerCase(ENGLISH);
        accessControl.checkCanDropRole(session.getRequiredTransactionId(), session.getIdentity(), role, catalog);
        Set<String> existingRoles = metadata.listRoles(session, catalog);
        if (!existingRoles.contains(role)) {
            throw new SemanticException(MISSING_ROLE, statement, "Role '%s' does not exist", role);
        }
        metadata.dropRole(session, role, catalog);
        return immediateFuture(null);
    }
}
