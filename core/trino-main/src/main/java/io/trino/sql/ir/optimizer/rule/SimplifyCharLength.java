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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.CharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.IrExpressions.constantNull;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.mayBeNull;

/**
 * Replaces {@code length(x)} where {@code x} is of type {@code char(n)} with its statically known
 * length {@code n}. A {@code CHAR(n)} value always has length {@code n}, so the call does not need
 * to be evaluated at runtime.
 * <p>
 * {@code length} returns null on null input. When {@code x} may be null, the call is therefore
 * rewritten to {@code IF(x IS NULL, null, n)} to preserve that semantics; only when {@code x} is
 * known to be non-null is it folded directly to the constant {@code n}.
 */
public class SimplifyCharLength
        implements IrOptimizerRule
{
    private final PlannerContext context;

    public SimplifyCharLength(PlannerContext context)
    {
        this.context = context;
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call(ResolvedFunction function, List<Expression> arguments))
                || !function.name().equals(builtinFunctionName("length"))
                || arguments.size() != 1) {
            return Optional.empty();
        }

        Expression argument = arguments.getFirst();
        if (!(argument.type() instanceof CharType charType)) {
            return Optional.empty();
        }

        Constant length = new Constant(BIGINT, (long) charType.getLength());
        if (mayBeNull(context, argument)) {
            return Optional.of(ifExpression(new IsNull(argument), constantNull(BIGINT), length));
        }
        return Optional.of(length);
    }
}
