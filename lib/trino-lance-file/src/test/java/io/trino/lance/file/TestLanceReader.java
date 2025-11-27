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
package io.trino.lance.file;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;

final class TestLanceReader
{
    private final LanceTester tester = new LanceTester();

    private static List<Double> doubleSequence(double start, double step, int items)
    {
        List<Double> values = new ArrayList<>(items);
        double nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values;
    }

    private static List<Float> floatSequence(float start, float step, int items)
    {
        ImmutableList.Builder<Float> values = ImmutableList.builder();
        float nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values.build();
    }

    @Test
    void testSmallNumericShortSequence()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17, 19, 23, 27)), 7));
    }

    @Test
    void testSmallNumericLongSequence()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17, 19, 23, 27)), 30_000));
    }

    @Test
    void testLargeNumeric()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE - 2)), 30_000));
    }

    @Test
    void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 30_000));
    }

    @Test
    void testFloatSequence()
            throws Exception
    {
        tester.testRoundTrip(REAL, floatSequence(0.0f, 0.1f, 30_000));
    }

    @Test
    void testStringLargeDictionary()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(doubleSequence(1.0, 0.001, 257)), 30_000)).stream().map(Object::toString).collect(toList()));
    }

    @Test
    void testStringSequence()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(doubleSequence(1.0, 10.01, 30_000)).stream().map(Object::toString).collect(toList()));
    }

    @Test
    void testStringDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream().map(Object::toString).collect(toList()));
    }

    @Test
    void testStringStrideDictionary()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123"))));
    }

    @Test
    void testStringConstant()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(nCopies(99999, "123")));
    }

    @Test
    void testLongList()
            throws Exception
    {
        // test preamble only chunks
        tester.testLongListRoundTrip(BIGINT, newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 10_000)).stream().map(Number::longValue).collect(toList()));
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values)
            throws Exception
    {
        List<Long> writeValues = ImmutableList.copyOf(values).stream().map(Number::longValue).collect(toList());

        tester.testRoundTrip(TINYINT, writeValues.stream().map(Long::byteValue)
                .collect(toList()));

        tester.testRoundTrip(SMALLINT, writeValues.stream().map(Long::shortValue)
                .collect(toList()));

        tester.testRoundTrip(INTEGER, writeValues.stream().map(Long::intValue)
                .collect(toList()));

        tester.testRoundTrip(BIGINT, writeValues);
    }
}
