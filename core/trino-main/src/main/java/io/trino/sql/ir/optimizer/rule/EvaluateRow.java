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
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

/**
 * Evaluates a constant Row expression
 */
public class EvaluateRow
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Row(List<Expression> fields, _)) || !fields.stream().allMatch(Constant.class::isInstance)) {
            return Optional.empty();
        }

        RowType rowType = (RowType) expression.type();
        return Optional.of(new Constant(
                rowType,
                buildRowValue(rowType, builders -> {
                    for (int i = 0; i < fields.size(); ++i) {
                        writeNativeValue(fields.get(i).type(), builders.get(i), ((Constant) fields.get(i)).value());
                    }
                })));
    }
}
