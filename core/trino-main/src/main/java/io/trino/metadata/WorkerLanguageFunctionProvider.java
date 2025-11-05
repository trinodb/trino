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

import com.google.inject.Inject;
import io.trino.execution.TaskId;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.LanguageFunctionEngine;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.sql.routine.SqlRoutineCompiler;
import io.trino.sql.routine.ir.IrRoutine;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class WorkerLanguageFunctionProvider
        implements LanguageFunctionProvider
{
    private final LanguageFunctionEngineManager languageFunctionEngineManager;
    private final Map<TaskId, Map<FunctionId, LanguageFunctionData>> queryFunctions = new ConcurrentHashMap<>();

    @Inject
    public WorkerLanguageFunctionProvider(LanguageFunctionEngineManager languageFunctionEngineManager)
    {
        this.languageFunctionEngineManager = requireNonNull(languageFunctionEngineManager, "languageFunctionEngineManager is null");
    }

    @Override
    public void registerTask(TaskId taskId, Map<FunctionId, LanguageFunctionData> functions)
    {
        queryFunctions.computeIfAbsent(taskId, _ -> functions);
    }

    @Override
    public void unregisterTask(TaskId taskId)
    {
        queryFunctions.remove(taskId);
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionId functionId, InvocationConvention invocationConvention, FunctionManager functionManager)
    {
        LanguageFunctionData data = queryFunctions.values().stream()
                .map(queryFunctions -> queryFunctions.get(functionId))
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown function implementation: " + functionId));

        if (data.definition().isPresent()) {
            LanguageFunctionDefinition definition = data.definition().get();

            LanguageFunctionEngine engine = languageFunctionEngineManager.getLanguageFunctionEngine(definition.language())
                    .orElseThrow(() -> new IllegalStateException("No language function engine for language: " + definition.language()));

            return engine.getScalarFunctionImplementation(
                    definition.returnType(),
                    definition.argumentTypes(),
                    definition.definition(),
                    definition.properties(),
                    invocationConvention);
        }

        // Recompile every time this function is called as the function dependencies may have changed.
        // The caller caches, so this should not be a problem.
        IrRoutine routine = data.irRoutine().orElseThrow();
        SpecializedSqlScalarFunction function = new SqlRoutineCompiler(functionManager).compile(routine);
        return function.getScalarFunctionImplementation(invocationConvention);
    }
}
