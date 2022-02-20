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

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;

/**
 * Operation.getChangeType() is package private
 */
public final class KuduOperationApplier
{
    private KuduOperationApplier()
    {
    }

    public static OperationResponse applyOperationAndVerifySucceeded(KuduSession kuduSession, Operation operation)
            throws KuduException
    {
        OperationResponse operationResponse = kuduSession.apply(operation);
        if (operationResponse != null && operationResponse.hasRowError()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Error while applying kudu operation %s: %s",
                    operation.getChangeType().toString(), operationResponse.getRowError()));
        }
        return operationResponse;
    }
}
