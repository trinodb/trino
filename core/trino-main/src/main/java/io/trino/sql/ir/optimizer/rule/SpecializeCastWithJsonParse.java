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

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.trino.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static io.trino.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;

public class SpecializeCastWithJsonParse
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public SpecializeCastWithJsonParse(PlannerContext context)
    {
        metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Cast(Call call, Type type) &&
                call.function().name().equals(builtinFunctionName("json_parse"))) {
            Expression string = call.arguments().getFirst();
            return switch (type) {
                case ArrayType arrayType -> Optional.of(new Call(metadata.getCoercion(builtinFunctionName(JSON_STRING_TO_ARRAY_NAME), string.type(), arrayType), call.arguments()));
                case MapType mapType -> Optional.of(new Call(metadata.getCoercion(builtinFunctionName(JSON_STRING_TO_MAP_NAME), string.type(), mapType), call.arguments()));
                case RowType rowType -> Optional.of(new Call(metadata.getCoercion(builtinFunctionName(JSON_STRING_TO_ROW_NAME), string.type(), rowType), call.arguments()));
                default -> Optional.empty();
            };
        }

        return Optional.empty();
    }
}
