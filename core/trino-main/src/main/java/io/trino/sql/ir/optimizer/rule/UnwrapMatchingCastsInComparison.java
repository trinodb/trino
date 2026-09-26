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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Between;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.bindIfNecessary;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchBetween;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.Logical.Operator.AND;

/// Remove matching casts from both operands of a comparison when the cast is an exact numeric widening. E.g,
///
/// - `CAST(a AS decimal(19, 2)) < CAST(b AS decimal(19, 2)) -> a < b` (for `decimal(18, 2)` a and b)
/// - `CAST(a AS decimal(19, 2)) BETWEEN CAST(b AS decimal(19, 2)) AND c -> a >= b AND CAST(a AS decimal(19, 2)) <= c`
///
/// An exact widening converts every source value without failure and keeps its numeric value, so the
/// sources compare the same way as the cast values.
public class UnwrapMatchingCastsInComparison
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public UnwrapMatchingCastsInComparison(PlannerContext context)
    {
        this.metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Let let && let.value() instanceof Cast value && matchBetween(let) instanceof Between between) {
            return unwrapBetween(session, symbolAllocator, value, between.min(), between.max());
        }

        if (matchComparison(expression) instanceof Comparison comparison
                && comparison.left() instanceof Cast left
                && comparison.right() instanceof Cast right
                && canUnwrap(left, right)) {
            return Optional.of(comparison(metadata, getCharVarcharCoercion(session), comparison.operator(), left.expression(), right.expression()));
        }

        return Optional.empty();
    }

    private Optional<Expression> unwrapBetween(Session session, SymbolAllocator symbolAllocator, Cast value, Expression min, Expression max)
    {
        if (!isMatchingCast(value, min) && !isMatchingCast(value, max)) {
            return Optional.empty();
        }

        return Optional.of(bindIfNecessary(symbolAllocator, "between", value.expression(), operand -> new Logical(AND, ImmutableList.of(
                boundComparison(session, GREATER_THAN_OR_EQUAL, value, operand, min),
                boundComparison(session, LESS_THAN_OR_EQUAL, value, operand, max)))));
    }

    private Expression boundComparison(Session session, ComparisonOperator operator, Cast value, Expression operand, Expression bound)
    {
        if (bound instanceof Cast boundCast && canUnwrap(value, boundCast)) {
            return comparison(metadata, getCharVarcharCoercion(session), operator, operand, boundCast.expression());
        }
        return comparison(metadata, getCharVarcharCoercion(session), operator, new Cast(operand, value.type(), value.kind()), bound);
    }

    private static boolean isMatchingCast(Cast value, Expression bound)
    {
        return bound instanceof Cast boundCast && canUnwrap(value, boundCast);
    }

    private static boolean canUnwrap(Cast left, Cast right)
    {
        Type sourceType = left.expression().type();
        return sourceType.equals(right.expression().type()) && isExactNumericWidening(sourceType, left.type());
    }

    private static boolean isExactNumericWidening(Type sourceType, Type targetType)
    {
        if (sourceType instanceof RealType) {
            return targetType instanceof DoubleType;
        }

        OptionalInt sourceDigits = requiredIntegerDigits(sourceType);
        OptionalInt targetDigits = exactIntegerDigits(targetType);
        if (sourceDigits.isEmpty() || targetDigits.isEmpty()) {
            return false;
        }

        return scale(targetType) >= scale(sourceType) && targetDigits.orElseThrow() >= sourceDigits.orElseThrow();
    }

    /// Returns the number of integer digits needed to hold every value of the given exact numeric type.
    private static OptionalInt requiredIntegerDigits(Type type)
    {
        return switch (type) {
            case TinyintType _ -> OptionalInt.of(3);
            case SmallintType _ -> OptionalInt.of(5);
            case IntegerType _ -> OptionalInt.of(10);
            case BigintType _ -> OptionalInt.of(19);
            case DecimalType decimalType -> OptionalInt.of(decimalType.getPrecision() - decimalType.getScale());
            default -> OptionalInt.empty();
        };
    }

    /// Returns the largest number of integer digits the given numeric type holds exactly for every value.
    private static OptionalInt exactIntegerDigits(Type type)
    {
        return switch (type) {
            case TinyintType _ -> OptionalInt.of(2);
            case SmallintType _ -> OptionalInt.of(4);
            case RealType _ -> OptionalInt.of(7);
            case IntegerType _ -> OptionalInt.of(9);
            case DoubleType _ -> OptionalInt.of(15);
            case BigintType _ -> OptionalInt.of(18);
            case DecimalType decimalType -> OptionalInt.of(decimalType.getPrecision() - decimalType.getScale());
            default -> OptionalInt.empty();
        };
    }

    private static int scale(Type type)
    {
        if (type instanceof DecimalType decimalType) {
            return decimalType.getScale();
        }
        return 0;
    }
}
