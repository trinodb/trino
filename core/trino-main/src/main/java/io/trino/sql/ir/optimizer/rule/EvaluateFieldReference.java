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
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.TypeUtils.readNativeValue;

/**
 * Resolves field reference from an explicit row. E.g.,
 * <ul>
 *     <li>{@code Row(a, b, c).0 -> a}
 *     <li>{@code Constant(<row type>, (1, 2, 3)).0 -> 1}
 * </ul>
 */
public class EvaluateFieldReference
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        return switch (expression) {
            case FieldReference(Row row, int field) -> Optional.of(row.items().get(field));
            case FieldReference(Constant(RowType type, SqlRow row), int field) -> {
                Type fieldType = type.getFields().get(field).getType();
                yield Optional.of(new Constant(
                        fieldType,
                        readNativeValue(fieldType, row.getRawFieldBlock(field), row.getRawIndex())));
            }
            case FieldReference(Constant(RowType type, Object _), int field) -> Optional.of(new Constant(type.getFields().get(field).getType(), null));
            default -> Optional.empty();
        };
    }
}
