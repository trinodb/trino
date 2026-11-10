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

import io.airlift.slice.Slices;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Measures building a discrete value set from unordered values, which is how
 * dynamic filters turn the values collected from the build side of a join into a domain.
 */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkValueSetCopyOf
{
    @Benchmark
    public ValueSet copyOf(Data data)
    {
        return ValueSet.copyOf(data.type, data.values);
    }

    @State(Scope.Thread)
    public static class Data
    {
        @Param({"10", "100", "1000", "10000", "50000"})
        public int size;

        @Param({"BIGINT", "INTEGER", "DATE", "VARCHAR", "DOUBLE", "DECIMAL"})
        public String typeName;

        @Param({"RANDOM", "SORTED", "REVERSE_SORTED", "FEW_DISTINCT"})
        public String pattern;

        private Type type;
        private List<Object> values;

        @Setup
        public void setup()
        {
            // fixed seed so that all runs and all code versions see identical values
            Random random = new Random(42);
            type = switch (typeName) {
                case "BIGINT" -> BIGINT;
                case "INTEGER" -> INTEGER;
                case "DATE" -> DATE;
                case "VARCHAR" -> VARCHAR;
                case "DOUBLE" -> DOUBLE;
                case "DECIMAL" -> createDecimalType(10, 2);
                default -> throw new IllegalArgumentException("Unsupported type: " + typeName);
            };

            // distinct values that are not in order, as the values collected from the build side of a join
            int distinctCount = pattern.equals("FEW_DISTINCT") ? Math.max(1, size / 20) : size;
            List<Object> distinctValues = new ArrayList<>(distinctCount);
            for (int i = 0; i < distinctCount; i++) {
                distinctValues.add(value(i * 7919L + random.nextInt(7919)));
            }
            Collections.shuffle(distinctValues, random);

            values = new ArrayList<>(size);
            switch (pattern) {
                case "RANDOM" -> values.addAll(distinctValues);
                case "SORTED" -> values.addAll(ascending(distinctValues));
                case "REVERSE_SORTED" -> {
                    values.addAll(ascending(distinctValues));
                    Collections.reverse(values);
                }
                case "FEW_DISTINCT" -> {
                    for (int i = 0; i < size; i++) {
                        values.add(distinctValues.get(random.nextInt(distinctValues.size())));
                    }
                }
                default -> throw new IllegalArgumentException("Unsupported pattern: " + pattern);
            }
        }

        private List<Object> ascending(List<Object> distinctValues)
        {
            // order the values by building the set of them, which is not what is being measured
            return SortedRangeSet.copyOf(type, distinctValues.stream().map(value -> Range.equal(type, value)).toList())
                    .getDiscreteSet();
        }

        private Object value(long number)
        {
            return switch (typeName) {
                case "BIGINT", "INTEGER", "DATE", "DECIMAL" -> number;
                case "VARCHAR" -> Slices.utf8Slice("value-" + number);
                case "DOUBLE" -> number / 8.0;
                default -> throw new IllegalArgumentException("Unsupported type: " + typeName);
            };
        }
    }

    @Test
    public void test()
    {
        for (String typeName : List.of("BIGINT", "INTEGER", "DATE", "VARCHAR", "DOUBLE", "DECIMAL")) {
            for (String pattern : List.of("RANDOM", "SORTED", "REVERSE_SORTED", "FEW_DISTINCT")) {
                Data data = new Data();
                data.size = 100;
                data.typeName = typeName;
                data.pattern = pattern;
                data.setup();

                copyOf(data);
            }
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkValueSetCopyOf.class).run();
    }
}
