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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBatchPageSortKeyPrefixFiller
{
    private static final int POSITION_COUNT = 999;

    private final TypeOperators typeOperators = new TypeOperators();
    private final OrderingCompiler orderingCompiler = new OrderingCompiler(typeOperators);

    @Test
    public void testLongTypes()
    {
        Random random = new Random(42);
        long[] values = new long[POSITION_COUNT];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextLong();
        }
        values[0] = Long.MIN_VALUE;
        values[1] = Long.MAX_VALUE;
        values[2] = 0;

        assertBatchMatchesInterpreted(BIGINT, new LongArrayBlock(POSITION_COUNT, Optional.empty(), values));
        assertBatchMatchesInterpreted(TIMESTAMP_MILLIS, new LongArrayBlock(POSITION_COUNT, Optional.empty(), values));
    }

    @Test
    public void testIntTypes()
    {
        Random random = new Random(42);
        int[] values = new int[POSITION_COUNT];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt();
        }
        values[0] = Integer.MIN_VALUE;
        values[1] = Integer.MAX_VALUE;
        values[2] = 0;

        assertBatchMatchesInterpreted(INTEGER, new IntArrayBlock(POSITION_COUNT, Optional.empty(), values));
        assertBatchMatchesInterpreted(DATE, new IntArrayBlock(POSITION_COUNT, Optional.empty(), values));
    }

    @Test
    public void testDouble()
    {
        Random random = new Random(42);
        long[] bits = new long[POSITION_COUNT];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = Double.doubleToLongBits(random.nextDouble() * 1e9 - 5e8);
        }
        bits[0] = Double.doubleToLongBits(Double.NaN);
        bits[1] = 0x7FF8_0000_0000_0001L; // non-canonical NaN
        bits[2] = 0xFFF8_0000_0000_0000L; // NaN with sign bit set
        bits[3] = 0xFFFF_FFFF_FFFF_FFFFL; // NaN with all bits set
        bits[4] = Double.doubleToLongBits(Double.POSITIVE_INFINITY);
        bits[5] = Double.doubleToLongBits(Double.NEGATIVE_INFINITY);
        bits[6] = Double.doubleToLongBits(0.0);
        bits[7] = Double.doubleToLongBits(-0.0);
        bits[8] = Double.doubleToLongBits(Double.MIN_VALUE);
        bits[9] = Double.doubleToLongBits(-Double.MIN_VALUE);

        assertBatchMatchesInterpreted(DOUBLE, new LongArrayBlock(POSITION_COUNT, Optional.empty(), bits));
    }

    @Test
    public void testReal()
    {
        Random random = new Random(42);
        int[] bits = new int[POSITION_COUNT];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = Float.floatToIntBits(random.nextFloat() * 1e9f - 5e8f);
        }
        bits[0] = Float.floatToIntBits(Float.NaN);
        bits[1] = 0x7F80_0001; // non-canonical NaN
        bits[2] = 0xFFC0_0000; // NaN with sign bit set
        bits[3] = 0xFFFF_FFFF; // NaN with all bits set
        bits[4] = Float.floatToIntBits(Float.POSITIVE_INFINITY);
        bits[5] = Float.floatToIntBits(Float.NEGATIVE_INFINITY);
        bits[6] = Float.floatToIntBits(0.0f);
        bits[7] = Float.floatToIntBits(-0.0f);

        assertBatchMatchesInterpreted(REAL, new IntArrayBlock(POSITION_COUNT, Optional.empty(), bits));
    }

    @Test
    public void testFallbackForNullsAndDictionaries()
    {
        Random random = new Random(42);
        long[] values = new long[POSITION_COUNT];
        boolean[] nulls = new boolean[POSITION_COUNT];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextLong();
            nulls[i] = random.nextInt(10) == 0;
        }
        Block blockWithNulls = new LongArrayBlock(POSITION_COUNT, Optional.of(nulls), values);
        assertBatchMatchesInterpreted(BIGINT, blockWithNulls);

        int[] ids = new int[POSITION_COUNT];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = random.nextInt(POSITION_COUNT);
        }
        Block dictionary = DictionaryBlock.create(POSITION_COUNT, new LongArrayBlock(POSITION_COUNT, Optional.empty(), values), ids);
        assertBatchMatchesInterpreted(BIGINT, dictionary);
    }

    private void assertBatchMatchesInterpreted(Type type, Block block)
    {
        Page page = new Page(block);
        for (SortOrder sortOrder : SortOrder.values()) {
            List<PageSortKeyPrefixFiller> fillers = orderingCompiler.compilePageSortKeyPrefixFillers(
                    ImmutableList.of(type), ImmutableList.of(0), ImmutableList.of(sortOrder));
            assertThat(fillers).hasSize(1);

            MethodHandle operator = typeOperators.getSortKeyPrefixOperator(type, sortOrder, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
            SortKeyPrefixFiller.SortKeyPrefixLayout layout = new SortKeyPrefixFiller.SortKeyPrefixLayout(
                    0, sortOrder, 64, false, 0);
            PageSortKeyPrefixFiller interpreted = new InterpretedPageSortKeyPrefixFiller(operator, layout);

            long[] actual = new long[POSITION_COUNT];
            long[] expected = new long[POSITION_COUNT];
            fillers.getFirst().fill(page, actual);
            interpreted.fill(page, expected);
            assertThat(actual).as("type %s, order %s", type, sortOrder).isEqualTo(expected);
        }
    }
}
