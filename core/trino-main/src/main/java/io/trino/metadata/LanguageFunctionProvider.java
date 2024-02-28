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
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;

import java.util.List;

public interface LanguageFunctionProvider
{
    LanguageFunctionProvider DISABLED = new LanguageFunctionProvider()
    {
        @Override
        public ScalarFunctionImplementation specialize(FunctionManager functionManager, ResolvedFunction resolvedFunction, FunctionDependencies functionDependencies, InvocationConvention invocationConvention)
        {
            throw new UnsupportedOperationException("SQL language functions are disabled");
        }

        @Override
        public void registerTask(TaskId taskId, List<LanguageScalarFunctionData> languageFunctions)
        {
            if (!languageFunctions.isEmpty()) {
                throw new UnsupportedOperationException("SQL language functions are disabled");
            }
        }

        @Override
        public void unregisterTask(TaskId taskId) {}
    };

    ScalarFunctionImplementation specialize(
            FunctionManager functionManager,
            ResolvedFunction resolvedFunction,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention);

    void registerTask(TaskId taskId, List<LanguageScalarFunctionData> languageFunctions);

    void unregisterTask(TaskId taskId);
}
