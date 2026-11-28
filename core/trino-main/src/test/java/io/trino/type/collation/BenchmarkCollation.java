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
package io.trino.type.collation;

import io.airlift.slice.Slice;
import io.trino.spi.type.Collation;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.invoke.MethodHandle;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.collation.CollationRegistry.collationRegistry;

/// The cost of comparing and hashing strings under a collation, against the default collation and
/// against comparing precomputed collation keys.
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCollation
{
    private static final int VALUES = 1024;

    @Param({"ucs_basic", "unicode", "unicode_ci"})
    private String collation;

    private Slice[] values;
    private Slice[] keys;
    private MethodHandle comparison;
    private MethodHandle hash;

    @Setup
    public void setup()
            throws Throwable
    {
        Random random = new Random(42);
        String[] words = {"Müller", "muller", "Straße", "STRASSE", "résumé", "Resume", "Ångström", "angstrom", "Zoë", "zoe", "Œuvre", "oeuvre"};
        values = new Slice[VALUES];
        for (int i = 0; i < VALUES; i++) {
            values[i] = utf8Slice(words[random.nextInt(words.length)] + " " + words[random.nextInt(words.length)]);
        }
        Collation value = collationRegistry().getCollation(collation);
        VarcharType type = VARCHAR.withCollation(value);
        TypeOperators typeOperators = new TypeOperators(collationRegistry());
        comparison = typeOperators.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
        hash = typeOperators.getXxHash64Operator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
        keys = new Slice[VALUES];
        for (int i = 0; i < VALUES; i++) {
            keys[i] = collationRegistry().comparison(value).collationKey(values[i]);
        }
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public long compare()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < VALUES; i++) {
            result += (long) comparison.invokeExact(values[i], values[(i + 1) % VALUES]);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public long compareKeys()
    {
        long result = 0;
        for (int i = 0; i < VALUES; i++) {
            result += keys[i].compareTo(keys[(i + 1) % VALUES]);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public long hash()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < VALUES; i++) {
            result += (long) hash.invokeExact(values[i]);
        }
        return result;
    }

    static void main()
            throws Throwable
    {
        BenchmarkCollation benchmark = new BenchmarkCollation();
        benchmark.collation = "unicode_ci";
        benchmark.setup();
        benchmark.compare();
        benchmark.hash();

        benchmark(BenchmarkCollation.class).run();
    }
}
