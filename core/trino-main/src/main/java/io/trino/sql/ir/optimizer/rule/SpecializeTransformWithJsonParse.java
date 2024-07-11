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
package io.trino.sql.ir.optimizer.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.operator.scalar.JsonStringArrayExtractScalar.JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;

/**
 * Optimize case of transformation with cast and json_extract_scalars E.g,
 *
 * <ul>
 *     <li>{@code transform(cast(json_parse(varchar_column) as array<json>), json -> json_extract_scalar(json, jsonPath)) -> $internal$json_string_array_extract_scalar(varchar_column, jsonPath)}</li>
 * </ul>
 */
public class SpecializeTransformWithJsonParse
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public SpecializeTransformWithJsonParse(PlannerContext context)
    {
        metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Call call
                && call.function().name().getFunctionName().equals(ARRAY_TRANSFORM_NAME)) {
            if (!(call.arguments().getFirst() instanceof Call transformArrayCall
                    && transformArrayCall.function().name().equals(builtinFunctionName(JSON_STRING_TO_ARRAY_NAME)))) {
                return Optional.empty();
            }

            if (call.arguments().getLast() instanceof Lambda transform
                    && transform.body() instanceof Call transformLambdaCall
                    && transformLambdaCall.function().name().equals(builtinFunctionName("json_extract_scalar"))) {
                Expression jsonData = transformArrayCall.arguments().getFirst();
                Constant jsonPath = (Constant) transformLambdaCall.arguments().getLast();
                Call newCall = new Call(
                        metadata.resolveBuiltinFunction(
                                JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME,
                                fromTypes(ImmutableList.of(jsonData.type(), jsonPath.type()))),
                        ImmutableList.of(jsonData, jsonPath));
                return Optional.of(newCall);
            }
        }
        return Optional.empty();
    }
}
