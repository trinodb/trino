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
import io.trino.sql.tree.Rollback;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.NOT_IN_TRANSACTION;
import static java.util.Objects.requireNonNull;

public class RollbackTask
        implements DataDefinitionTask<Rollback>
{
    private final TransactionManager transactionManager;

    @Inject
    public RollbackTask(TransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public String getName()
    {
        return "ROLLBACK";
    }

    @Override
    public ListenableFuture<Void> execute(
            Rollback statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        TransactionId transactionId = session.getTransactionId().orElseThrow(() -> new TrinoException(NOT_IN_TRANSACTION, "No transaction in progress"));

        stateMachine.clearTransactionId();
        transactionManager.asyncAbort(transactionId);
        return immediateVoidFuture();
    }
}
