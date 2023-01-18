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
package org.apache.kudu.client;

import io.trino.plugin.kudu.KuduClientSession;
import io.trino.plugin.kudu.KuduClientWrapper;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static org.apache.kudu.client.SessionConfiguration.FlushMode.MANUAL_FLUSH;

/**
 * Not thread safe
 * This class is used to buffer operations and apply them in a batch and verify the batch was successfully applied.
 * This gives:
 * - performance of not flushing after every operation
 * - correctness of not flushing in the background and hoping operations succeed
 * The buffer is flushed when:
 * - The KuduOperationApplier is closed
 * - Or if the KuduOperationApplier reaches the bufferMaxOperations
 * Note: Operation.getChangeType() is package private
 */
public final class KuduOperationApplier
        implements AutoCloseable
{
    private static final int bufferMaxOperations = 1000;

    private int currentOperationsInBuffer;
    private final KuduSession kuduSession;

    private KuduOperationApplier(KuduSession kuduSession)
    {
        kuduSession.setFlushMode(MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(bufferMaxOperations);
        this.kuduSession = kuduSession;
        currentOperationsInBuffer = 0;
    }

    public static KuduOperationApplier fromKuduClientWrapper(KuduClientWrapper kuduClientWrapper)
    {
        KuduSession session = kuduClientWrapper.newSession();
        return new KuduOperationApplier(session);
    }

    public static KuduOperationApplier fromKuduClientSession(KuduClientSession kuduClientSession)
    {
        KuduSession session = kuduClientSession.newSession();
        return new KuduOperationApplier(session);
    }

    /**
     * Not thread safe
     * Applies an operation without waiting for it to be flushed, operations are flushed in the background
     *
     * @param operation kudu operation
     */
    public void applyOperationAsync(Operation operation)
            throws KuduException
    {
        if (currentOperationsInBuffer >= bufferMaxOperations) {
            List<OperationResponse> operationResponses = kuduSession.flush();
            verifyNoErrors(operationResponses);
        }
        OperationResponse operationResponse = kuduSession.apply(operation);
        checkState(operationResponse == null, "KuduSession must be configured with MANUAL_FLUSH mode");
        currentOperationsInBuffer += 1;
    }

    private void verifyNoErrors(List<OperationResponse> operationResponses)
    {
        List<FailedOperation> failedOperations = operationResponses.stream()
                .map(FailedOperation::fromOperationResponse)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (!failedOperations.isEmpty()) {
            FailedOperation firstError = failedOperations.get(0);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Error while applying %s kudu operation(s); First error: %s: %s",
                    failedOperations.size(),
                    firstError.operationResponse.getOperation().getChangeType().toString(),
                    firstError.rowError));
        }
        currentOperationsInBuffer = 0;
    }

    @Override
    public void close()
            throws KuduException
    {
        List<OperationResponse> operationResponses = kuduSession.close();
        verifyNoErrors(operationResponses);
    }

    private static class FailedOperation
    {
        public RowError rowError;
        public OperationResponse operationResponse;

        public static Optional<FailedOperation> fromOperationResponse(OperationResponse operationResponse)
        {
            return Optional.ofNullable(operationResponse)
                    .flatMap(response -> Optional.ofNullable(response.getRowError()))
                    .map(rowError -> new FailedOperation(operationResponse, rowError));
        }

        private FailedOperation(OperationResponse operationResponse, RowError rowError)
        {
            this.operationResponse = operationResponse;
            this.rowError = rowError;
        }
    }
}
