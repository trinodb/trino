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
import io.trino.spi.TrinoException;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.Expression;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;

import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_IN_TRANSACTION;

public class CommitTask
        implements DataDefinitionTask<Commit>
{
    @Override
    public String getName()
    {
        return "COMMIT";
    }

    @Override
    public ListenableFuture<Void> execute(
            Commit statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        if (session.getTransactionId().isEmpty()) {
            throw new TrinoException(NOT_IN_TRANSACTION, "No transaction in progress");
        }
        TransactionId transactionId = session.getTransactionId().get();

        stateMachine.clearTransactionId();
        return transactionManager.asyncCommit(transactionId);
    }
}
