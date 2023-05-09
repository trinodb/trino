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
package io.trino.operator.table.json.execution;

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.json.ir.IrJsonPath;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;

public class ValueColumn
        implements Column
{
    private final int outputIndex;
    private final MethodHandle methodHandle;
    private final IrJsonPath path;
    private final long emptyBehavior;
    private final int emptyDefaultInput;
    private final long errorBehavior;
    private final int errorDefaultInput;
    private final Type resultType;

    public ValueColumn(
            int outputIndex,
            MethodHandle methodHandle,
            IrJsonPath path,
            long emptyBehavior,
            int emptyDefaultInput,
            long errorBehavior,
            int errorDefaultInput,
            Type resultType)
    {
        this.outputIndex = outputIndex;
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.path = requireNonNull(path, "path is null");
        this.emptyBehavior = emptyBehavior;
        this.emptyDefaultInput = emptyDefaultInput;
        this.errorBehavior = errorBehavior;
        this.errorDefaultInput = errorDefaultInput;
        this.resultType = requireNonNull(resultType, "resultType is null");
    }

    @Override
    public Object evaluate(long sequentialNumber, JsonNode item, Page input, int position)
    {
        Object emptyDefault;
        if (emptyDefaultInput == -1) {
            emptyDefault = null;
        }
        else {
            emptyDefault = readNativeValue(resultType, input.getBlock(emptyDefaultInput), position);
        }

        Object errorDefault;
        if (errorDefaultInput == -1) {
            errorDefault = null;
        }
        else {
            errorDefault = readNativeValue(resultType, input.getBlock(errorDefaultInput), position);
        }

        try {
            return methodHandle.invoke(item, path, null, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
        }
        catch (Throwable throwable) {
            // According to ISO/IEC 9075-2:2016(E) 7.11 <JSON table> p.462 General rules 1) e) ii) 2) D) any exception thrown by column evaluation should be propagated.
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public int getOutputIndex()
    {
        return outputIndex;
    }
}
