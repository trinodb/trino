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
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.security.Identity;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.SetSessionAuthorization;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.transaction.TransactionManager;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class SetSessionAuthorizationTask
        implements DataDefinitionTask<SetSessionAuthorization>
{
    @Override
    public String getName()
    {
        return "SET SESSION AUTHORIZATION";
    }

    @Override
    public ListenableFuture<?> execute(SetSessionAuthorization statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        Identity originalIdentity = session.getOriginalIdentity();

        session.getTransactionId().ifPresent(transactionId -> {
            if (!transactionManager.isAutoCommit(transactionId)) {
                throw new PrestoException(GENERIC_USER_ERROR, "Can't set authorization user in the middle of a transaction");
            }
        });

        String user;
        Expression userExpression = statement.getUser();
        if (userExpression instanceof Identifier) {
            user = ((Identifier) userExpression).getValue();
        }
        else if (userExpression instanceof StringLiteral) {
            user = ((StringLiteral) userExpression).getValue();
        }
        else {
            throw new IllegalArgumentException("Unsupported user expression: " + userExpression.getClass().getName());
        }
        checkState(!isNullOrEmpty(user), "Authorization user cannot be null or empty");

        if (!originalIdentity.getUser().equals(user)) {
            accessControl.checkCanSetUser(originalIdentity.getPrincipal(), user);
            accessControl.checkCanImpersonateUser(originalIdentity, user);
        }
        stateMachine.addSetAuthorizationUser(user);
        return immediateFuture(null);
    }
}
