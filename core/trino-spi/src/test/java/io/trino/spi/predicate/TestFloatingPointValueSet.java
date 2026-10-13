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
package io.trino.spi.predicate;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static io.trino.spi.predicate.FloatingPointValueSet.nanValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFloatingPointValueSet
{
    @Test
    void testMixedFactoryOperations()
    {
        for (Type type : List.of(DOUBLE, REAL, NUMBER)) {
            IntFunction<Object> value = finiteValue(type);
            ValueSet ranges = SortedRangeSet.copyOf(type, List.of(Range.range(type, value.apply(0), true, value.apply(2), true)));
            ValueSet point = ValueSet.of(type, value.apply(1));
            assertThat(ranges.intersect(point)).isEqualTo(point);
            assertThat(point.intersect(ranges)).isEqualTo(point);
            assertThat(ranges.union(point)).isEqualTo(ValueSet.ofRanges(Range.range(type, value.apply(0), true, value.apply(2), true)));
            assertThat(ranges.union(point)).isEqualTo(point.union(ranges));
            assertThat(ranges.contains(point)).isTrue();
            assertThat(point.contains(ranges)).isFalse();
            assertThat(ranges.overlaps(point)).isTrue();
            assertThat(point.overlaps(ranges)).isTrue();
            assertThat(ranges.subtract(point)).isEqualTo(ValueSet.ofRanges(
                    Range.range(type, value.apply(0), true, value.apply(1), false),
                    Range.range(type, value.apply(1), false, value.apply(2), true)));
            assertThat(point.subtract(ranges).isNone()).isTrue();
            assertThat(ranges.union(List.of(point, ValueSet.of(type, value.apply(3))))).isEqualTo(ValueSet.ofRanges(
                    Range.range(type, value.apply(0), true, value.apply(2), true),
                    Range.equal(type, value.apply(3))));
        }
    }

    @Test
    void testMixedRepresentationMembership()
    {
        for (Type type : List.of(DOUBLE, REAL, NUMBER)) {
            IntFunction<Object> value = finiteValue(type);
            Object nan = nanValue(type);
            SortedRangeSet ordered = FloatingPointValueSet.allOrderedValues(type);
            List<Object> samples = List.of(
                    value.apply(-1),
                    value.apply(0),
                    value.apply(1),
                    value.apply(2),
                    value.apply(3),
                    ordered.getSpan().getLowBoundedValue(),
                    ordered.getSpan().getHighBoundedValue(),
                    nan);
            List<SortedRangeSet> legacy = List.of(
                    SortedRangeSet.none(type),
                    SortedRangeSet.all(type),
                    SortedRangeSet.copyOf(type, List.of(Range.equal(type, value.apply(1)))),
                    SortedRangeSet.copyOf(type, List.of(Range.lessThan(type, value.apply(0)))),
                    SortedRangeSet.copyOf(type, List.of(Range.greaterThan(type, value.apply(0)))),
                    ordered);
            ValueSet point = ValueSet.of(type, value.apply(1));
            ValueSet nanOnly = ValueSet.of(type, nan);
            List<ValueSet> modern = List.of(ValueSet.none(type), ValueSet.all(type), point, point.complement(), nanOnly, nanOnly.complement(), point.union(nanOnly));
            for (ValueSet ranges : legacy) {
                for (ValueSet floatingPoint : modern) {
                    for (List<ValueSet> operands : List.of(List.of(ranges, floatingPoint), List.of(floatingPoint, ranges))) {
                        ValueSet left = operands.get(0);
                        ValueSet right = operands.get(1);
                        ValueSet intersection = left.intersect(right);
                        ValueSet union = left.union(right);
                        ValueSet difference = left.subtract(right);
                        assertThat(left.union(List.of(right, left))).isEqualTo(union);
                        assertThat(left.contains(right)).isEqualTo(samples.stream().allMatch(sample -> !right.containsValue(sample) || left.containsValue(sample)));
                        assertThat(left.overlaps(right)).isEqualTo(samples.stream().anyMatch(sample -> right.containsValue(sample) && left.containsValue(sample)));
                        for (Object sample : samples) {
                            assertThat(intersection.containsValue(sample)).isEqualTo(left.containsValue(sample) && right.containsValue(sample));
                            assertThat(union.containsValue(sample)).isEqualTo(left.containsValue(sample) || right.containsValue(sample));
                            assertThat(difference.containsValue(sample)).isEqualTo(left.containsValue(sample) && !right.containsValue(sample));
                        }
                    }
                }
            }
        }
    }

    private static IntFunction<Object> finiteValue(Type type)
    {
        if (type.equals(DOUBLE)) {
            return value -> (double) value;
        }
        if (type.equals(REAL)) {
            return value -> (long) floatToRawIntBits(value);
        }
        return value -> TrinoNumber.from(BigDecimal.valueOf(value));
    }

    @Test
    void testSetAlgebra()
    {
        for (Type type : List.of(DOUBLE, REAL, NUMBER)) {
            Object zero = type.equals(DOUBLE) ? 0.0 : type.equals(REAL) ? (long) floatToRawIntBits(0.0f) : TrinoNumber.from(BigDecimal.ZERO);
            Object nan = nanValue(type);
            ValueSet point = ValueSet.of(type, zero);
            ValueSet nanOnly = ValueSet.of(type, nan);
            List<ValueSet> sets = List.of(ValueSet.none(type), ValueSet.all(type), point, nanOnly, point.union(nanOnly), point.complement(), nanOnly.complement());
            for (ValueSet left : sets) {
                assertThat(left.union(left.complement())).isEqualTo(ValueSet.all(type));
                assertThat(left.intersect(left.complement())).isEqualTo(ValueSet.none(type));
                assertThat(left.complement().complement()).isEqualTo(left);
                for (ValueSet right : sets) {
                    assertThat(left.contains(right)).isEqualTo(right.subtract(left).isNone());
                    assertThat(left.overlaps(right)).isEqualTo(!left.intersect(right).isNone());
                    assertThat(left.union(right)).isEqualTo(right.union(left));
                    assertThat(left.union(List.of(right, left))).isEqualTo(left.union(right));
                    assertThat(left.intersect(right)).isEqualTo(right.intersect(left));
                    assertThat(left.union(right).complement()).isEqualTo(left.complement().intersect(right.complement()));
                    for (Object value : List.of(zero, nan)) {
                        assertThat(left.union(right).containsValue(value)).isEqualTo(left.containsValue(value) || right.containsValue(value));
                        assertThat(left.intersect(right).containsValue(value)).isEqualTo(left.containsValue(value) && right.containsValue(value));
                        assertThat(left.subtract(right).containsValue(value)).isEqualTo(left.containsValue(value) && !right.containsValue(value));
                    }
                }
            }
            Domain domain = Domain.create(point.union(nanOnly), true);
            assertThat(domain.includesNullableValue(null)).isTrue();
            assertThat(domain.includesNullableValue(zero)).isTrue();
            assertThat(domain.includesNullableValue(nan)).isTrue();
            assertThat(domain.complement().includesNullableValue(nan)).isFalse();
        }
    }

    @Test
    void testNaNSingletonAndRanges()
    {
        ValueSet nan = ValueSet.of(DOUBLE, Double.NaN);
        assertThat(nan.isSingleValue()).isTrue();
        assertThat(nan.getSingleValue()).isEqualTo(Double.NaN);
        assertThat(nan.getDiscreteSet()).containsExactly(Double.NaN);
        assertThat(nan.tryExpandRanges(1)).hasValue(List.of(Double.NaN));
        assertThatThrownBy(nan::getRanges).isInstanceOf(IllegalStateException.class);
        assertThat(nan.getValuesProcessor().transform(_ -> false, _ -> false, _ -> false, FloatingPointValueSet::isNaNAllowed)).isTrue();
        assertThat(nan.containsValue(Double.longBitsToDouble(0x7FF8000000000001L))).isTrue();
        ValueSet ordered = ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 0.0), Range.greaterThan(DOUBLE, 0.0));
        assertThat(ordered.containsValue(Double.NaN)).isFalse();
        assertThat(ordered.containsValue(Double.NEGATIVE_INFINITY)).isTrue();
        assertThat(ordered.containsValue(Double.POSITIVE_INFINITY)).isTrue();
        assertThat(ordered.complement()).isEqualTo(nan);
        assertThatThrownBy(() -> nan.union(ValueSet.of(BIGINT, 1L))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInfinityRanges()
    {
        for (Type type : List.of(REAL, DOUBLE, NUMBER)) {
            Range universe = FloatingPointValueSet.allOrderedValues(type).getSpan();
            for (Object infinity : List.of(universe.getLowBoundedValue(), universe.getHighBoundedValue())) {
                ValueSet singleton = ValueSet.of(type, infinity);
                assertThat(singleton.getRanges().getOrderedRanges()).containsExactly(Range.equal(type, infinity));
                assertThat(singleton.getRanges().getSpan().isLowUnbounded()).isFalse();
                assertThat(singleton.getRanges().getSpan().isHighUnbounded()).isFalse();
            }
            ValueSet infinities = ValueSet.of(type, universe.getLowBoundedValue(), universe.getHighBoundedValue());
            assertThat(infinities.getRanges().getOrderedRanges()).containsExactly(
                    Range.equal(type, universe.getLowBoundedValue()),
                    Range.equal(type, universe.getHighBoundedValue()));
        }
    }

    @Test
    void testSimplificationPreservesNaN()
    {
        Domain withNaN = Domain.multipleValues(DOUBLE, List.of(1.0, 3.0, Double.NaN));
        Domain withoutNaN = Domain.multipleValues(DOUBLE, List.of(1.0, 3.0));
        assertThat(withNaN.simplify(1).includesNullableValue(Double.NaN)).isTrue();
        assertThat(withoutNaN.simplify(1).includesNullableValue(Double.NaN)).isFalse();
        assertThat(withNaN.simplify(1).contains(withNaN)).isTrue();
        assertThat(withoutNaN.simplify(1).contains(withoutNaN)).isTrue();
    }

    @Test
    void testSerialization()
            throws Exception
    {
        JsonMapper mapper = new JsonMapperProvider()
                .withJsonDeserializers(Map.of(
                        Type.class, new TestingTypeDeserializer(new TestingTypeManager()),
                        Block.class, new TestingBlockJsonSerde.Deserializer(new TestingBlockEncodingSerde())))
                .withJsonSerializers(Map.of(
                        Block.class, new TestingBlockJsonSerde.Serializer(new TestingBlockEncodingSerde())))
                .get();
        for (Type type : List.of(DOUBLE, REAL, NUMBER)) {
            Object nan = nanValue(type);
            for (Domain domain : List.of(Domain.none(type), Domain.all(type), Domain.singleValue(type, nan), Domain.singleValue(type, nan).complement())) {
                assertThat(mapper.readValue(mapper.writeValueAsBytes(domain), Domain.class)).isEqualTo(domain);
            }
        }
        // The old serialized range representation remains readable and retains its NaN membership.
        String oldValues = mapper.writerFor(ValueSet.class).writeValueAsString(SortedRangeSet.all(DOUBLE));
        Domain restored = mapper.readValue("{\"values\":" + oldValues + ",\"nullAllowed\":false}", Domain.class);
        assertThat(restored).isEqualTo(Domain.notNull(DOUBLE));
    }
}
