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
package io.trino.sql.planner;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedAggregationFunctionMetadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.AggregationDecomposition;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static java.util.Objects.requireNonNull;

/// Resolves the functions implementing an aggregation's decomposition into steps that exchange
/// intermediate state: the partial function consumes raw input and produces the intermediate type,
/// the output function consumes the intermediate type and produces the final result, and the
/// intermediate function consumes and produces the intermediate type.
///
/// For a function with a declared decomposition ([AggregationDecomposition]) these are separately
/// resolved functions (e.g. `count` decomposes into `count` and `$sum0`); a legacy decomposition
/// reuses the original function at every step and switches behavior on the step at execution time.
public final class AggregationDecompositions
{
    private AggregationDecompositions() {}

    /// How a single-step aggregation function splits into a partial and an output step.
    public record DecomposedAggregation(ResolvedFunction partialFunction, ResolvedFunction outputFunction, Type intermediateType, boolean legacyDecomposition, List<AggregationDecomposition.SubsumedFunction> subsumed)
    {
        public DecomposedAggregation
        {
            requireNonNull(partialFunction, "partialFunction is null");
            requireNonNull(outputFunction, "outputFunction is null");
            requireNonNull(intermediateType, "intermediateType is null");
            subsumed = List.copyOf(requireNonNull(subsumed, "subsumed is null"));
        }
    }

    public static DecomposedAggregation decompose(Metadata metadata, TypeManager typeManager, Session session, ResolvedFunction function)
    {
        ResolvedAggregationFunctionMetadata functionMetadata = metadata.getAggregationFunctionMetadata(session, function);
        if (functionMetadata.decomposition().isEmpty()) {
            List<Type> intermediateTypes = functionMetadata.intermediateTypes().stream()
                    .map(typeManager::getType)
                    .collect(toImmutableList());
            Type intermediateType = intermediateTypes.size() == 1 ? intermediateTypes.getFirst() : RowType.anonymous(intermediateTypes);
            return new DecomposedAggregation(function, function, intermediateType, true, List.of());
        }

        AggregationDecomposition decomposition = functionMetadata.decomposition().get();
        ResolvedFunction partialFunction = metadata.resolveBuiltinFunction(decomposition.partial(), fromTypes(function.signature().getArgumentTypes()));
        Type intermediateType = partialFunction.signature().getReturnType();
        ResolvedFunction outputFunction = metadata.resolveBuiltinFunction(decomposition.output(), fromTypes(intermediateType));
        return new DecomposedAggregation(partialFunction, outputFunction, intermediateType, false, decomposition.subsumed());
    }

    /// Resolves the intermediate function for a function in partial position (consuming raw input).
    /// Partial results are combined by the partial function's declared output function resolved over
    /// the intermediate type (e.g. partial `count`s are combined with `$sum0`). The function must
    /// have a declared decomposition.
    public static ResolvedFunction resolveIntermediateFromPartial(Metadata metadata, Session session, ResolvedFunction partialFunction)
    {
        AggregationDecomposition decomposition = getDecomposition(metadata, session, partialFunction);
        return metadata.resolveBuiltinFunction(decomposition.output(), fromTypes(partialFunction.signature().getReturnType()));
    }

    /// Resolves the intermediate function for a function in final position (consuming the
    /// intermediate type and producing the final result). Intermediate state is combined by the
    /// output function's declared partial function resolved over its argument types. The function
    /// must have a declared decomposition.
    public static ResolvedFunction resolveIntermediateFromOutput(Metadata metadata, Session session, ResolvedFunction outputFunction)
    {
        AggregationDecomposition decomposition = getDecomposition(metadata, session, outputFunction);
        return metadata.resolveBuiltinFunction(decomposition.partial(), fromTypes(outputFunction.signature().getArgumentTypes()));
    }

    private static AggregationDecomposition getDecomposition(Metadata metadata, Session session, ResolvedFunction function)
    {
        return metadata.getAggregationFunctionMetadata(session, function)
                .decomposition()
                .orElseThrow(() -> new IllegalStateException("Declared decomposition is missing for aggregation function: " + function.signature().getName()));
    }
}
