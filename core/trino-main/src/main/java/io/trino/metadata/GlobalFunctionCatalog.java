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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.window.WindowFunctionSupplier;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.TypeSignature;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.isOperatorName;
import static io.trino.metadata.Signature.unmangleOperator;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Locale.ENGLISH;

@ThreadSafe
public class GlobalFunctionCatalog
{
    public static final String GLOBAL_CATALOG = "system";
    public static final String GLOBAL_SCHEMA = "global";
    private volatile FunctionMap functions = new FunctionMap();

    public final synchronized void addFunctions(FunctionBundle functionBundle)
    {
        for (FunctionMetadata functionMetadata : functionBundle.getFunctions()) {
            checkArgument(!functionMetadata.getSignature().getName().contains("|"), "Function name cannot contain '|' character: %s", functionMetadata.getSignature());
            checkArgument(!functionMetadata.getSignature().getName().contains("@"), "Function name cannot contain '@' character: %s", functionMetadata.getSignature());
            checkNotSpecializedTypeOperator(functionMetadata.getSignature());
            for (FunctionMetadata existingFunction : this.functions.list()) {
                checkArgument(!functionMetadata.getFunctionId().equals(existingFunction.getFunctionId()), "Function already registered: %s", functionMetadata.getFunctionId());
                checkArgument(!functionMetadata.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", functionMetadata.getSignature());
            }
        }
        this.functions = new FunctionMap(this.functions, functionBundle);
    }

    /**
     * Type operators are handled automatically by the engine, so custom operator implementations
     * cannot be registered for these.
     */
    private static void checkNotSpecializedTypeOperator(Signature signature)
    {
        String name = signature.getName();
        if (!isOperatorName(name)) {
            return;
        }

        OperatorType operatorType = unmangleOperator(name);

        // The trick here is the Generic*Operator implementations implement these exact signatures,
        // so we only these exact signatures to be registered.  Since, only a single function with
        // a specific signature can be registered, it prevents others from being registered.
        Signature.Builder expectedSignature = Signature.builder()
                .name(signature.getName())
                .argumentTypes(Collections.nCopies(operatorType.getArgumentCount(), new TypeSignature("T")));

        switch (operatorType) {
            case EQUAL:
            case IS_DISTINCT_FROM:
            case INDETERMINATE:
                expectedSignature.returnType(BOOLEAN);
                expectedSignature.comparableTypeParameter("T");
                break;
            case HASH_CODE:
            case XX_HASH_64:
                expectedSignature.returnType(BIGINT);
                expectedSignature.comparableTypeParameter("T");
                break;
            case COMPARISON_UNORDERED_FIRST:
            case COMPARISON_UNORDERED_LAST:
                expectedSignature.returnType(INTEGER);
                expectedSignature.orderableTypeParameter("T");
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                expectedSignature.returnType(BOOLEAN);
                expectedSignature.orderableTypeParameter("T");
                break;
            default:
                return;
        }

        checkArgument(signature.equals(expectedSignature.build()), "Can not register %s functionMetadata: %s", operatorType, signature);
    }

    public List<FunctionMetadata> listFunctions()
    {
        return functions.list();
    }

    public Collection<FunctionMetadata> getFunctions(SchemaFunctionName name)
    {
        if (!GLOBAL_SCHEMA.equals(name.getSchemaName())) {
            return ImmutableList.of();
        }
        return functions.get(name.getFunctionName());
    }

    public FunctionMetadata getFunctionMetadata(FunctionId functionId)
    {
        return functions.get(functionId);
    }

    public AggregationFunctionMetadata getAggregationFunctionMetadata(FunctionId functionId)
    {
        return functions.getFunctionBundle(functionId).getAggregationFunctionMetadata(functionId);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return functions.getFunctionBundle(functionId).getWindowFunctionImplementation(functionId, boundSignature, functionDependencies);
    }

    public AggregationMetadata getAggregateFunctionImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return functions.getFunctionBundle(functionId).getAggregateFunctionImplementation(functionId, boundSignature, functionDependencies);
    }

    public FunctionDependencyDeclaration getFunctionDependencies(FunctionId functionId, BoundSignature boundSignature)
    {
        return functions.getFunctionBundle(functionId).getFunctionDependencies(functionId, boundSignature);
    }

    public FunctionInvoker getScalarFunctionInvoker(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        return functions.getFunctionBundle(functionId).getScalarFunctionInvoker(functionId, boundSignature, functionDependencies, invocationConvention);
    }

    private static class FunctionMap
    {
        private final Map<FunctionId, FunctionBundle> functionBundlesById;
        private final Map<FunctionId, FunctionMetadata> functionsById;
        // function names are currently lower cased
        private final Multimap<String, FunctionMetadata> functionsByLowerCaseName;

        public FunctionMap()
        {
            functionBundlesById = ImmutableMap.of();
            functionsById = ImmutableMap.of();
            functionsByLowerCaseName = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, FunctionBundle functionBundle)
        {
            this.functionBundlesById = ImmutableMap.<FunctionId, FunctionBundle>builder()
                    .putAll(map.functionBundlesById)
                    .putAll(functionBundle.getFunctions().stream()
                            .collect(toImmutableMap(FunctionMetadata::getFunctionId, functionMetadata -> functionBundle)))
                    .buildOrThrow();

            this.functionsById = ImmutableMap.<FunctionId, FunctionMetadata>builder()
                    .putAll(map.functionsById)
                    .putAll(functionBundle.getFunctions().stream()
                            .collect(toImmutableMap(FunctionMetadata::getFunctionId, Function.identity())))
                    .buildOrThrow();

            ImmutableListMultimap.Builder<String, FunctionMetadata> functionsByName = ImmutableListMultimap.<String, FunctionMetadata>builder()
                    .putAll(map.functionsByLowerCaseName);
            functionBundle.getFunctions()
                    .forEach(functionMetadata -> functionsByName.put(functionMetadata.getSignature().getName().toLowerCase(ENGLISH), functionMetadata));
            this.functionsByLowerCaseName = functionsByName.build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<String, Collection<FunctionMetadata>> entry : this.functionsByLowerCaseName.asMap().entrySet()) {
                Collection<FunctionMetadata> values = entry.getValue();
                long aggregations = values.stream()
                        .map(FunctionMetadata::getKind)
                        .filter(kind -> kind == AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<FunctionMetadata> list()
        {
            return ImmutableList.copyOf(functionsByLowerCaseName.values());
        }

        public Collection<FunctionMetadata> get(String functionName)
        {
            return functionsByLowerCaseName.get(functionName.toLowerCase(ENGLISH));
        }

        public FunctionMetadata get(FunctionId functionId)
        {
            FunctionMetadata functionMetadata = functionsById.get(functionId);
            checkArgument(functionMetadata != null, "Unknown function implementation: " + functionId);
            return functionMetadata;
        }

        public FunctionBundle getFunctionBundle(FunctionId functionId)
        {
            FunctionBundle functionBundle = functionBundlesById.get(functionId);
            checkArgument(functionBundle != null, "Unknown function implementation: " + functionId);
            return functionBundle;
        }
    }
}
