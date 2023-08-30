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
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.sql.routine.SqlRoutineCompiler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class WorkerLanguageFunctionProvider
        implements LanguageFunctionProvider
{
    private final Map<TaskId, Map<ResolvedFunction, LanguageScalarFunctionData>> queryFunctions = new ConcurrentHashMap<>();

    @Override
    public void registerTask(TaskId taskId, List<LanguageScalarFunctionData> functions)
    {
        queryFunctions.computeIfAbsent(taskId, ignored -> functions.stream().collect(toImmutableMap(LanguageScalarFunctionData::resolvedFunction, Function.identity())));
    }

    @Override
    public void unregisterTask(TaskId taskId)
    {
        queryFunctions.remove(taskId);
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionManager functionManager, ResolvedFunction resolvedFunction, FunctionDependencies functionDependencies, InvocationConvention invocationConvention)
    {
        LanguageScalarFunctionData functionData = queryFunctions.values().stream()
                .map(queryFunctions -> queryFunctions.get(resolvedFunction))
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown function implementation: " + resolvedFunction.getFunctionId()));

        // Recompile every time this function is called as the function dependencies may have changed.
        // The caller caches, so this should not be a problem.
        // TODO: compiler should use function dependencies instead of function manager
        SpecializedSqlScalarFunction function = new SqlRoutineCompiler(functionManager).compile(functionData.routine());
        return function.getScalarFunctionImplementation(invocationConvention);
    }
}
