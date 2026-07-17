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

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.sql.ir.Expression;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Utilities for the {@code $engine_expression} synthetic function, which wraps an engine IR
 * expression as an opaque {@link ConnectorExpression} payload for connectors that support the
 * {@link io.trino.spi.connector.ConnectorExpressionEvaluator} SPI.
 */
public final class EngineExpressions
{
    public static final FunctionName ENGINE_EXPRESSION_FUNCTION_NAME = new FunctionName("$engine_expression");

    private EngineExpressions() {}

    /**
     * Returns {@code true} if {@code expression} contains at least one
     * {@code $engine_expression} node anywhere in its subtree.
     */
    public static boolean containsEngineExpression(ConnectorExpression expression)
    {
        if (expression instanceof Call call && call.getFunctionName().equals(ENGINE_EXPRESSION_FUNCTION_NAME)) {
            return true;
        }
        return expression.getChildren().stream().anyMatch(EngineExpressions::containsEngineExpression);
    }

    /**
     * Wraps {@code predicate} as a {@code $engine_expression(payload, ...variables)} call.
     * The payload is the JSON-serialized IR expression; the variable arguments let connectors
     * discover which columns the expression references.
     */
    public static ConnectorExpression buildEngineExpression(Expression predicate, JsonCodec<Expression> serializer)
    {
        List<Variable> predicateVariables = SymbolsExtractor.extractUnique(predicate).stream()
                .map(symbol -> new Variable(symbol.name(), symbol.type()))
                .collect(toImmutableList());
        return new Call(
                BOOLEAN,
                ENGINE_EXPRESSION_FUNCTION_NAME,
                ImmutableList.<ConnectorExpression>builder()
                        .add(new Constant(Slices.utf8Slice(serializer.toJson(predicate)), VARCHAR))
                        .addAll(predicateVariables)
                        .build());
    }
}
