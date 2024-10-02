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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.IrUtils.or;

/**
 * Simplify IN expression with continuous range of constant test values into a BETWEEN expression. E.g,
 * <ul>
 *     <li>{@code $in(x, [1, 2, 3, 4]) -> $between(x, 1, 4)}
 * </ul>
 */
public class SimplifyContinuousInValues
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof In in)) {
            return Optional.empty();
        }

        if (in.valueList().size() < 2) {
            return Optional.empty();
        }

        Type valueType = in.value().type();
        if (!isDirectLongComparisonValidForContinuousValues(valueType)) {
            return Optional.empty();
        }

        if (valueType.getJavaType() != long.class) {
            return Optional.empty();
        }

        boolean nullMatch = false;
        long nonNullsCount = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (Expression testExpression : in.valueList()) {
            if (!(testExpression instanceof Constant constant)) {
                return Optional.empty();
            }
            if (constant.value() == null) {
                nullMatch = true;
                continue;
            }
            long longConstant = (long) constant.value();
            min = Math.min(min, longConstant);
            max = Math.max(max, longConstant);
            nonNullsCount++;
        }

        // If all values within a range are included, use a range filter
        if (nonNullsCount >= 2 && areAllValuesInRangeIncluded(max, min, nonNullsCount)) {
            Between between = new Between(in.value(), new Constant(valueType, min), new Constant(valueType, max));
            if (nullMatch) {
                return Optional.of(or(new IsNull(in.value()), between));
            }
            return Optional.of(between);
        }

        return Optional.empty();
    }

    private static boolean isDirectLongComparisonValidForContinuousValues(Type type)
    {
        // Types for which we can safely use equality and comparison on the stored long value
        // instead of going through type specific methods and where the next consecutive value
        // can be obtained by incrementing the stored long value by 1
        return type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                (type instanceof TimeType timeType && timeType.getPrecision() == 12) ||
                type instanceof DateType ||
                (type instanceof TimestampType timestampType && timestampType.getPrecision() == 6) ||
                (type instanceof DecimalType decimalType && decimalType.isShort());
    }

    private static boolean areAllValuesInRangeIncluded(long max, long min, long nonNullsCount)
    {
        BigInteger range = BigInteger.valueOf(max)
                .subtract(BigInteger.valueOf(min))
                .add(BigInteger.valueOf(1));
        try {
            return range.longValueExact() == nonNullsCount;
        }
        catch (ArithmeticException e) {
            return false;
        }
    }
}
