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
package io.trino.metadata;

import io.trino.execution.TaskId;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.sql.routine.ir.IrRoutine;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public interface LanguageFunctionProvider
{
    LanguageFunctionProvider DISABLED = new LanguageFunctionProvider()
    {
        @Override
        public ScalarFunctionImplementation specialize(FunctionId functionId, InvocationConvention invocationConvention, FunctionManager functionManager)
        {
            throw new UnsupportedOperationException("SQL language functions are disabled");
        }

        @Override
        public void registerTask(TaskId taskId, Map<FunctionId, LanguageFunctionData> languageFunctions)
        {
            if (!languageFunctions.isEmpty()) {
                throw new UnsupportedOperationException("SQL language functions are disabled");
            }
        }

        @Override
        public void unregisterTask(TaskId taskId) {}
    };

    ScalarFunctionImplementation specialize(FunctionId functionId, InvocationConvention invocationConvention, FunctionManager functionManager);

    void registerTask(TaskId taskId, Map<FunctionId, LanguageFunctionData> languageFunctions);

    void unregisterTask(TaskId taskId);

    record LanguageFunctionData(Optional<IrRoutine> irRoutine, Optional<LanguageFunctionDefinition> definition)
    {
        public LanguageFunctionData
        {
            requireNonNull(irRoutine, "irRoutine is null");
            requireNonNull(definition, "definition is null");
            checkArgument(irRoutine.isPresent() != definition.isPresent(), "exactly one of irRoutine and metadata must be present");
        }

        public static LanguageFunctionData ofIrRoutine(IrRoutine irRoutine)
        {
            return new LanguageFunctionData(Optional.of(irRoutine), Optional.empty());
        }

        public static LanguageFunctionData ofDefinition(LanguageFunctionDefinition metadata)
        {
            return new LanguageFunctionData(Optional.empty(), Optional.of(metadata));
        }
    }
}
