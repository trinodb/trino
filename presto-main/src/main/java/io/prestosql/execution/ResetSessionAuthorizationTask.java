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
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ResetSessionAuthorization;
import io.prestosql.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class ResetSessionAuthorizationTask
        implements DataDefinitionTask<ResetSessionAuthorization>
{
    @Override
    public String getName()
    {
        return "RESET SESSION AUTHORIZATION";
    }

    @Override
    public ListenableFuture<?> execute(ResetSessionAuthorization statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();

        session.getTransactionId().ifPresent(transactionId -> {
            if (!transactionManager.isAutoCommit(transactionId)) {
                throw new PrestoException(GENERIC_USER_ERROR, "Can't reset authorization user in the middle of a transaction");
            }
        });

        stateMachine.resetAuthorizationUser();
        return immediateFuture(null);
    }
}
