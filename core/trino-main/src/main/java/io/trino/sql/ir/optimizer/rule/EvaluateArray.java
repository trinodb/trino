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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.TypeUtils.writeNativeValue;

/**
 * Evaluates a constant Array expression
 */
public class EvaluateArray
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Array(Type elementType, List<Expression> elements)) || !elements.stream().allMatch(Constant.class::isInstance)) {
            return Optional.empty();
        }

        BlockBuilder builder = elementType.createBlockBuilder(null, elements.size());
        for (Expression element : elements) {
            writeNativeValue(elementType, builder, ((Constant) element).value());
        }

        return Optional.of(new Constant(expression.type(), builder.build()));
    }
}
