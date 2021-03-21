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
package io.trino.spi.connector;

import static io.trino.spi.connector.MergeDetails.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeDetails.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeDetails.UPDATE_OPERATION_NUMBER;

public enum MergeCaseKind
{
    INSERT(INSERT_OPERATION_NUMBER),
    DELETE(DELETE_OPERATION_NUMBER),
    UPDATE(UPDATE_OPERATION_NUMBER);

    private final int operationNumber;

    MergeCaseKind(int operationNumber)
    {
        this.operationNumber = operationNumber;
    }

    public int getOperationNumber()
    {
        return operationNumber;
    }

    public boolean matchedKind()
    {
        return this != INSERT;
    }
}
