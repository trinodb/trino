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
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class StartTransactionTask
        implements DataDefinitionTask<StartTransaction>
{
    private final TransactionManager transactionManager;

    @Inject
    public StartTransactionTask(TransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public String getName()
    {
        return "START TRANSACTION";
    }

    @Override
    public ListenableFuture<Void> execute(
            StartTransaction statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        if (!session.isClientTransactionSupport()) {
            throw new TrinoException(StandardErrorCode.INCOMPATIBLE_CLIENT, "Client does not support transactions");
        }
        if (session.getTransactionId().isPresent()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Nested transactions not supported");
        }

        Optional<IsolationLevel> isolationLevel = extractIsolationLevel(statement);
        Optional<Boolean> readOnly = extractReadOnly(statement);

        TransactionId transactionId = transactionManager.beginTransaction(
                isolationLevel.orElse(TransactionManager.DEFAULT_ISOLATION),
                readOnly.orElse(TransactionManager.DEFAULT_READ_ONLY),
                false);

        stateMachine.setStartedTransactionId(transactionId);

        // Since the current session does not contain this new transaction ID, we need to manually mark it as inactive
        // when this statement completes.
        transactionManager.trySetInactive(transactionId);

        return immediateVoidFuture();
    }

    private static Optional<IsolationLevel> extractIsolationLevel(StartTransaction startTransaction)
    {
        if (startTransaction.getTransactionModes().stream()
                .filter(Isolation.class::isInstance)
                .count() > 1) {
            throw semanticException(SYNTAX_ERROR, startTransaction, "Multiple transaction isolation levels specified");
        }

        return startTransaction.getTransactionModes().stream()
                .filter(Isolation.class::isInstance)
                .map(Isolation.class::cast)
                .map(Isolation::getLevel)
                .map(StartTransactionTask::convertLevel)
                .findFirst();
    }

    private static Optional<Boolean> extractReadOnly(StartTransaction startTransaction)
    {
        if (startTransaction.getTransactionModes().stream()
                .filter(TransactionAccessMode.class::isInstance)
                .count() > 1) {
            throw semanticException(SYNTAX_ERROR, startTransaction, "Multiple transaction read modes specified");
        }

        return startTransaction.getTransactionModes().stream()
                .filter(TransactionAccessMode.class::isInstance)
                .map(TransactionAccessMode.class::cast)
                .map(TransactionAccessMode::isReadOnly)
                .findFirst();
    }

    private static IsolationLevel convertLevel(Isolation.Level level)
    {
        switch (level) {
            case SERIALIZABLE:
                return IsolationLevel.SERIALIZABLE;
            case REPEATABLE_READ:
                return IsolationLevel.REPEATABLE_READ;
            case READ_COMMITTED:
                return IsolationLevel.READ_COMMITTED;
            case READ_UNCOMMITTED:
                return IsolationLevel.READ_UNCOMMITTED;
        }
        throw new AssertionError("Unhandled isolation level: " + level);
    }
}
