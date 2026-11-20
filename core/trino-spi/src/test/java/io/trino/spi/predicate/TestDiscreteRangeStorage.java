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

import io.airlift.json.JsonMapperProvider;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDiscreteRangeStorage
{
    @Test
    public void testRetainedSizeAndLegacyViews()
    {
        SortedRangeSet compact = SortedRangeSet.of(BIGINT, IntStream.range(0, 10_000).mapToObj(value -> (long) value).toList());
        long retainedSize = compact.getRetainedSizeInBytes();
        assertThat(retainedSize).isLessThan(85_000);
        assertThat(compact.getSortedRanges().getPositionCount()).isEqualTo(20_000);
        assertThat(compact.getInclusive()).hasSize(20_000).containsOnly(true);
        assertThat(compact.getOrderedRanges()).hasSize(10_000);
        assertThat(compact.getSpan()).isEqualTo(Range.range(BIGINT, 0L, true, 9_999L, true));
        assertThat(compact.getRetainedSizeInBytes()).isEqualTo(retainedSize);
    }

    @Test
    public void testEqualityAcrossStorageForms()
    {
        for (Type type : List.of(BIGINT, VARCHAR, DOUBLE, REAL, NUMBER)) {
            List<Object> values = IntStream.range(0, 100).mapToObj(index -> value(type, index * 2)).toList();
            SortedRangeSet compact = SortedRangeSet.of(type, values);
            SortedRangeSet legacy = SortedRangeSet.copyOf(type, values.stream().map(value -> Range.equal(type, value)).toList());
            assertThat(compact).isEqualTo(legacy);
            assertThat(legacy).isEqualTo(compact);
            assertThat(compact.hashCode()).isEqualTo(legacy.hashCode());
            assertThat(legacy.isDiscreteSet()).isTrue();
            assertThat(compact).isEqualTo(legacy);
            assertThat(compact.getRetainedSizeInBytes()).isLessThan(legacy.getRetainedSizeInBytes());
            assertThat(compact.complement()).isEqualTo(legacy.complement());
        }
    }

    @Test
    public void testMixedStorageOperations()
    {
        Random random = new Random(91837);
        for (Type type : List.of(BIGINT, VARCHAR, DOUBLE, REAL, NUMBER)) {
            for (int iteration = 0; iteration < 100; iteration++) {
                List<Object> leftValues = IntStream.range(0, 20).mapToObj(_ -> value(type, random.nextInt(40))).toList();
                List<Object> rightValues = IntStream.range(0, 20).mapToObj(_ -> value(type, random.nextInt(40))).toList();
                SortedRangeSet left = SortedRangeSet.of(type, leftValues);
                SortedRangeSet right = SortedRangeSet.of(type, rightValues);
                SortedRangeSet legacyLeft = SortedRangeSet.copyOf(type, leftValues.stream().map(value -> Range.equal(type, value)).toList());
                SortedRangeSet legacyRight = SortedRangeSet.copyOf(type, rightValues.stream().map(value -> Range.equal(type, value)).toList());
                List<Range> union = new ArrayList<>(left.getOrderedRanges());
                union.addAll(right.getOrderedRanges());
                SortedRangeSet expectedUnion = SortedRangeSet.copyOf(type, union);
                SortedRangeSet expectedIntersection = legacyLeft.linearSearchIntersect(legacyRight);
                for (SortedRangeSet first : List.of(left, legacyLeft)) {
                    for (SortedRangeSet second : List.of(right, legacyRight)) {
                        assertThat(first.union(second)).isEqualTo(expectedUnion);
                        assertThat(first.intersect(second)).isEqualTo(expectedIntersection);
                        assertThat(first.overlaps(second)).isEqualTo(!expectedIntersection.isNone());
                        assertThat(first.contains(second)).isEqualTo(expectedIntersection.equals(second));
                    }
                }
                assertThat(legacyRight.isDiscreteSet()).isTrue();
                assertThat(left.unionRanges(List.of(right, legacyRight))).isEqualTo(expectedUnion);
                SortedRangeSet interval = SortedRangeSet.of(Range.range(type, value(type, 10), false, value(type, 30), true));
                assertThat(left.intersect(interval)).isEqualTo(legacyLeft.linearSearchIntersect(interval));
                assertThat(interval.intersect(left)).isEqualTo(legacyLeft.linearSearchIntersect(interval));
                union.addAll(interval.getOrderedRanges());
                assertThat(left.unionRanges(List.of(right, interval))).isEqualTo(SortedRangeSet.copyOf(type, union));
                assertThat(left.complement()).isEqualTo(legacyLeft.complement());
            }
        }
    }

    @Test
    public void testSkewedDiscreteIntersection()
    {
        for (Type type : List.of(BIGINT, VARCHAR, DOUBLE, REAL, NUMBER)) {
            List<Object> values = IntStream.range(0, 1000).mapToObj(index -> value(type, index * 2)).toList();
            SortedRangeSet compact = SortedRangeSet.of(type, values);
            SortedRangeSet legacy = SortedRangeSet.copyOf(type, values.stream().map(value -> Range.equal(type, value)).toList());
            for (SortedRangeSet candidate : List.of(
                    SortedRangeSet.of(Range.range(type, value(type, 100), false, value(type, 300), false)),
                    SortedRangeSet.of(Range.range(type, value(type, 101), true, value(type, 301), true)),
                    SortedRangeSet.of(Range.lessThan(type, value(type, 0))),
                    SortedRangeSet.of(Range.greaterThan(type, value(type, 1998))),
                    SortedRangeSet.of(Range.greaterThanOrEqual(type, value(type, 1998))),
                    SortedRangeSet.of(Range.lessThanOrEqual(type, value(type, 100)), Range.greaterThan(type, value(type, 1000))),
                    SortedRangeSet.of(type, value(type, 0), value(type, 1000), value(type, 1998)))) {
                SortedRangeSet expected = legacy.linearSearchIntersect(candidate);
                assertThat(compact.intersect(candidate)).isEqualTo(expected);
                assertThat(candidate.intersect(compact)).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testSerializationCompatibility()
    {
        JsonMapper mapper = new JsonMapperProvider()
                .withJsonDeserializers(Map.of(
                        Type.class, new TestingTypeDeserializer(new TestingTypeManager()),
                        Block.class, new TestingBlockJsonSerde.Deserializer(new TestingBlockEncodingSerde())))
                .withJsonSerializers(Map.of(Block.class, new TestingBlockJsonSerde.Serializer(new TestingBlockEncodingSerde())))
                .get();
        for (Type type : List.of(BIGINT, VARCHAR, DOUBLE, REAL, NUMBER)) {
            List<Object> values = IntStream.range(0, 100).mapToObj(index -> value(type, index * 2)).toList();
            SortedRangeSet compact = SortedRangeSet.of(type, values);
            SortedRangeSet legacy = SortedRangeSet.copyOf(type, values.stream().map(value -> Range.equal(type, value)).toList());
            assertThat(legacy.isDiscreteSet()).isTrue();
            for (SortedRangeSet source : List.of(compact, legacy)) {
                SortedRangeSet restored = mapper.readValue(mapper.writeValueAsBytes(source), SortedRangeSet.class);
                assertThat(restored).isEqualTo(compact);
                assertThat(restored.hashCode()).isEqualTo(source.hashCode());
                assertThat(restored.getRetainedSizeInBytes()).isLessThan(legacy.getRetainedSizeInBytes());
            }
        }
        ObjectNode invalid = mapper.valueToTree(SortedRangeSet.of(Range.range(BIGINT, 1L, true, 2L, true)));
        invalid.put("discreteSetMarker", "DISCRETE");
        assertThatThrownBy(() -> mapper.treeToValue(invalid, SortedRangeSet.class))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Invalid bounds for discrete value at position 0");
    }

    private static Object value(Type type, int value)
    {
        if (type.equals(BIGINT)) {
            return (long) value;
        }
        if (type.equals(VARCHAR)) {
            return utf8Slice("%012d".formatted(value));
        }
        if (type.equals(DOUBLE)) {
            return (double) value;
        }
        if (type.equals(REAL)) {
            return (long) floatToRawIntBits(value);
        }
        return TrinoNumber.from(BigDecimal.valueOf(value));
    }
}
