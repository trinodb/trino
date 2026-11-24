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
package io.trino.type;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.TrinoNumber.AsBigDecimal.COMPARE_NAN_LAST;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkNumberOperators
{
    private static final int POSITIONS = 1024;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Param({"4", "18", "38"})
    private int digits;

    // 0 keeps all values at the same scale
    @Param({"0", "3", "30"})
    private int scaleSpread;

    private ValueBlock left;
    private ValueBlock right;
    private byte[] fixed;
    private byte[] variable;
    private int[] variableOffsets;

    private MethodHandle blockHash;
    private MethodHandle blockHashViaStack;
    private MethodHandle flatHash;
    private MethodHandle flatHashViaStack;
    private MethodHandle blockBlockEqual;
    private MethodHandle blockBlockEqualViaStack;
    private MethodHandle flatBlockEqual;
    private MethodHandle flatBlockEqualViaStack;
    private MethodHandle blockBlockComparison;
    private MethodHandle blockBlockLessThan;
    private MethodHandle readBlock;

    @Setup
    public void setup()
            throws Throwable
    {
        Random random = new Random(42);
        BlockBuilder leftBuilder = NUMBER.createBlockBuilder(null, POSITIONS);
        BlockBuilder rightBuilder = NUMBER.createBlockBuilder(null, POSITIONS);
        for (int i = 0; i < POSITIONS; i++) {
            TrinoNumber value = randomNumber(random);
            NUMBER.writeObject(leftBuilder, value);
            // half equal, half not
            NUMBER.writeObject(rightBuilder, random.nextBoolean() ? value : randomNumber(random));
        }
        left = leftBuilder.buildValueBlock();
        right = rightBuilder.buildValueBlock();

        fixed = new byte[NUMBER.getFlatFixedSize() * POSITIONS];
        variableOffsets = new int[POSITIONS];
        int variableSize = 0;
        for (int i = 0; i < POSITIONS; i++) {
            variableOffsets[i] = variableSize;
            variableSize += NUMBER.getFlatVariableWidthSize(left, i);
        }
        variable = new byte[variableSize];
        MethodHandle writeFlat = TYPE_OPERATORS.getReadValueOperator(NUMBER, simpleConvention(FLAT_RETURN, VALUE_BLOCK_POSITION));
        for (int i = 0; i < POSITIONS; i++) {
            writeFlat.invokeExact(left, i, fixed, i * NUMBER.getFlatFixedSize(), variable, variableOffsets[i]);
        }

        readBlock = TYPE_OPERATORS.getReadValueOperator(NUMBER, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle readFlat = TYPE_OPERATORS.getReadValueOperator(NUMBER, simpleConvention(FAIL_ON_NULL, FLAT));
        MethodHandle stackHash = TYPE_OPERATORS.getXxHash64Operator(NUMBER, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
        MethodHandle stackEqual = TYPE_OPERATORS.getEqualOperator(NUMBER, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));

        blockHash = TYPE_OPERATORS.getXxHash64Operator(NUMBER, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        blockHashViaStack = collectArguments(stackHash, 0, readBlock);
        flatHash = TYPE_OPERATORS.getXxHash64Operator(NUMBER, simpleConvention(FAIL_ON_NULL, FLAT));
        flatHashViaStack = collectArguments(stackHash, 0, readFlat);
        blockBlockEqual = TYPE_OPERATORS.getEqualOperator(NUMBER, simpleConvention(NULLABLE_RETURN, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        blockBlockEqualViaStack = collectArguments(collectArguments(stackEqual, 0, readBlock), 2, readBlock);
        flatBlockEqual = TYPE_OPERATORS.getEqualOperator(NUMBER, simpleConvention(NULLABLE_RETURN, FLAT, VALUE_BLOCK_POSITION_NOT_NULL));
        flatBlockEqualViaStack = collectArguments(collectArguments(stackEqual, 0, readFlat), 4, readBlock);
        blockBlockComparison = TYPE_OPERATORS.getComparisonUnorderedLastOperator(NUMBER, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        blockBlockLessThan = TYPE_OPERATORS.getLessThanOperator(NUMBER, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
    }

    private TrinoNumber randomNumber(Random random)
    {
        BigInteger unscaled = new BigInteger(digits * 10 / 3, random);
        BigDecimal value = new BigDecimal(unscaled, digits / 2 + random.nextInt(scaleSpread + 1));
        return TrinoNumber.from(random.nextBoolean() ? value : value.negate());
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long blockHash()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            result += (long) blockHash.invokeExact(left, i);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long blockHashViaStack()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            result += (long) blockHashViaStack.invokeExact(left, i);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long flatHash()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            result += (long) flatHash.invokeExact(fixed, i, variable, variableOffsets[i]);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long flatHashViaStack()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            result += (long) flatHashViaStack.invokeExact(fixed, i, variable, variableOffsets[i]);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long blockBlockEqual()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            if ((Boolean) blockBlockEqual.invokeExact(left, i, right, i)) {
                result++;
            }
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long blockBlockEqualViaStack()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            if ((Boolean) blockBlockEqualViaStack.invokeExact(left, i, right, i)) {
                result++;
            }
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long flatBlockEqual()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            if ((Boolean) flatBlockEqual.invokeExact(fixed, i, variable, variableOffsets[i], right, i)) {
                result++;
            }
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long flatBlockEqualViaStack()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            if ((Boolean) flatBlockEqualViaStack.invokeExact(fixed, i, variable, variableOffsets[i], right, i)) {
                result++;
            }
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long blockBlockComparison()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            result += (long) blockBlockComparison.invokeExact(left, i, right, i);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long blockBlockComparisonViaBigDecimal()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            TrinoNumber leftValue = (TrinoNumber) readBlock.invokeExact(left, i);
            TrinoNumber rightValue = (TrinoNumber) readBlock.invokeExact(right, i);
            result += COMPARE_NAN_LAST.compare(leftValue.toBigDecimal(), rightValue.toBigDecimal());
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long blockBlockLessThan()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            if ((boolean) blockBlockLessThan.invokeExact(left, i, right, i)) {
                result++;
            }
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long blockBlockLessThanViaBigDecimal()
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < POSITIONS; i++) {
            TrinoNumber leftValue = (TrinoNumber) readBlock.invokeExact(left, i);
            TrinoNumber rightValue = (TrinoNumber) readBlock.invokeExact(right, i);
            TrinoNumber.AsBigDecimal leftDecimal = leftValue.toBigDecimal();
            TrinoNumber.AsBigDecimal rightDecimal = rightValue.toBigDecimal();
            if (!(leftDecimal instanceof TrinoNumber.NotANumber) && !(rightDecimal instanceof TrinoNumber.NotANumber) && COMPARE_NAN_LAST.compare(leftDecimal, rightDecimal) < 0) {
                result++;
            }
        }
        return result;
    }

    @Test
    public void testBenchmark()
            throws Throwable
    {
        for (int digits : List.of(4, 18, 38)) {
            for (int scaleSpread : List.of(0, 3, 30)) {
                BenchmarkNumberOperators benchmark = new BenchmarkNumberOperators();
                benchmark.digits = digits;
                benchmark.scaleSpread = scaleSpread;
                benchmark.setup();
                // verify each benchmark is in sync with its baseline, computing the same result
                assertThat(benchmark.blockHash()).isEqualTo(benchmark.blockHashViaStack());
                assertThat(benchmark.flatHash()).isEqualTo(benchmark.flatHashViaStack());
                assertThat(benchmark.blockBlockEqual()).isEqualTo(benchmark.blockBlockEqualViaStack());
                assertThat(benchmark.flatBlockEqual()).isEqualTo(benchmark.flatBlockEqualViaStack());
                assertThat(benchmark.blockBlockLessThan()).isEqualTo(benchmark.blockBlockLessThanViaBigDecimal());
                benchmark.blockBlockComparison();
                benchmark.blockBlockComparisonViaBigDecimal();
            }
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkNumberOperators.class).run();
    }
}
