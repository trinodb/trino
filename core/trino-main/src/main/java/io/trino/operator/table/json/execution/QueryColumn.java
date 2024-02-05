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

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class QueryColumn
        implements Column
{
    private final int outputIndex;
    private final MethodHandle methodHandle;
    private final IrJsonPath path;
    private final long wrapperBehavior;
    private final long emptyBehavior;
    private final long errorBehavior;

    public QueryColumn(int outputIndex, MethodHandle methodHandle, IrJsonPath path, long wrapperBehavior, long emptyBehavior, long errorBehavior)
    {
        this.outputIndex = outputIndex;
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.path = requireNonNull(path, "path is null");
        this.wrapperBehavior = wrapperBehavior;
        this.emptyBehavior = emptyBehavior;
        this.errorBehavior = errorBehavior;
    }

    @Override
    public Object evaluate(long sequentialNumber, JsonNode item, Page input, int position)
    {
        try {
            return methodHandle.invoke(item, path, null, wrapperBehavior, emptyBehavior, errorBehavior);
        }
        catch (Throwable throwable) {
            // According to ISO/IEC 9075-2:2016(E) 7.11 <JSON table> p.462 General rules 1) e) ii) 3) D) any exception thrown by column evaluation should be propagated.
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
