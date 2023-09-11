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
import io.trino.spi.TrinoException;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ResetSessionAuthorization;
import io.trino.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class ResetSessionAuthorizationTask
        implements DataDefinitionTask<ResetSessionAuthorization>
{
    private final TransactionManager transactionManager;

    @Inject
    public ResetSessionAuthorizationTask(TransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public String getName()
    {
        return "RESET SESSION AUTHORIZATION";
    }

    @Override
    public ListenableFuture<Void> execute(
            ResetSessionAuthorization statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        session.getTransactionId().ifPresent(transactionId -> {
            if (!transactionManager.getTransactionInfo(transactionId).isAutoCommitContext()) {
                throw new TrinoException(GENERIC_USER_ERROR, "Can't reset authorization user in the middle of a transaction");
            }
        });
        stateMachine.resetAuthorizationUser();
        return immediateFuture(null);
    }
}
