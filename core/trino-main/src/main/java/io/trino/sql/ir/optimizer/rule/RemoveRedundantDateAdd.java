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

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.TimeField;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.DateTimeFunctions.isValidDateUnit;
import static io.trino.operator.scalar.DateTimeFunctions.isValidTimestampUnit;

/**
 * Replaces {@code date_add(unit, 0, x)} with {@code x}. Adding zero of any unit leaves the value
 * unchanged, so the call does not need to be evaluated at runtime.
 * <p>
 * The unit must be a constant that {@code date_add} accepts for {@code x}'s type, so that a call
 * with an invalid unit keeps failing.
 */
public class RemoveRedundantDateAdd
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Call(ResolvedFunction function, List<Expression> arguments)
                && function.name().equals(builtinFunctionName("date_add"))
                && arguments.size() == 3) {
            Expression unitExpression = arguments.get(0);
            Expression amountExpression = arguments.get(1);
            Expression dateTime = arguments.get(2);
            if (unitExpression instanceof Constant(VarcharType _, Slice unit)
                    && amountExpression instanceof Constant(Type _, Long amount)
                    && amount == 0
                    && isKnownValidUnit(dateTime.type(), unit)) {
                return Optional.of(dateTime);
            }
        }
        return Optional.empty();
    }

    private static boolean isKnownValidUnit(Type type, Slice unit)
    {
        return switch (type) {
            case DateType _ -> isValidDateUnit(unit);
            case TimeType _, TimeWithTimeZoneType _ -> TimeField.isValidUnit(unit);
            case TimestampType _, TimestampWithTimeZoneType _ -> isValidTimestampUnit(unit);
            default -> false;
        };
    }
}
