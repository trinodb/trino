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
package io.trino.operator.scalar.preimage;

import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongPredicate;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public final class IntegralToFloatingPointCastPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = resultDomain.getType();
        Comparator<Object> comparison = context.functions().resultComparator();
        Optional<Function<Object, Object>> forward = context.functions().coercion(source, target);
        Optional<Function<Object, Object>> reverse = context.functions().coercion(target, source);
        if (forward.isEmpty() || reverse.isEmpty()) {
            return Optional.empty();
        }
        return CastPreimages.compute(() -> CastPreimages.orderedRanges(resultDomain, source, (value, lower, inclusive) -> {
            if (injectiveAt(source, target, value)) {
                return CastPreimages.roundTripBound(source, source.getRange(), forward.get(), reverse.get(), comparison, value, lower, inclusive);
            }
            return bound(source, forward.get(), comparison, value, lower, inclusive);
        }));
    }

    static boolean supports(Type source, Type target)
    {
        return (source.equals(BIGINT) && target.equals(DOUBLE)) || ((source.equals(BIGINT) || source.equals(INTEGER)) && target.equals(REAL));
    }

    private static Optional<ValueSet> bound(Type source, Function<Object, Object> forward, Comparator<Object> comparison, Object value, boolean lower, boolean inclusive)
    {
        Type.Range range = source.getRange().orElseThrow();
        long low = (long) range.getMin();
        long high = (long) range.getMax();
        // Find the first input above the boundary. Looking at the complete fiber is
        // necessary when adjacent integers round to the same floating-point value.
        boolean strict = lower ? !inclusive : inclusive;
        LongPredicate above = input -> {
            int order = comparison.compare(forward.apply(input), value);
            return strict ? order > 0 : order >= 0;
        };
        if (above.test(low)) {
            return Optional.of(lower ? ValueSet.all(source) : ValueSet.none(source));
        }
        if (!above.test(high)) {
            return Optional.of(lower ? ValueSet.none(source) : ValueSet.all(source));
        }
        while (low < high) {
            long middle = (low & high) + ((low ^ high) >> 1);
            if (above.test(middle)) {
                high = middle;
            }
            else {
                low = middle + 1;
            }
        }
        return Optional.of(ValueSet.ofRanges(lower ? Range.greaterThanOrEqual(source, low) : Range.lessThan(source, low)));
    }

    static boolean injectiveAt(Type source, Type target, Object value)
    {
        if (source.equals(BIGINT) && target.equals(DOUBLE)) {
            double number = (double) value;
            return number > Long.MAX_VALUE || number < Long.MIN_VALUE || (number > -(1L << 53) && number < (1L << 53));
        }
        if ((source.equals(BIGINT) || source.equals(INTEGER)) && target.equals(REAL)) {
            float number = intBitsToFloat(toIntExact((long) value));
            return (source.equals(BIGINT) && (number > Long.MAX_VALUE || number < Long.MIN_VALUE)) ||
                    (source.equals(INTEGER) && (number > Integer.MAX_VALUE || number < Integer.MIN_VALUE)) ||
                    (number > -(1L << 23) && number < (1L << 23));
        }
        return true;
    }
}
