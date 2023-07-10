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
package io.trino.operator.aggregation;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.operator.aggregation.AggregationMaskCompiler.generateAggregationMaskBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAggregationMaskCompiler
{
    @DataProvider
    public Object[][] maskBuilderSuppliers()
    {
        Supplier<AggregationMaskBuilder> interpretedMaskBuilderSupplier = () -> new InterpretedAggregationMaskBuilder(1);
        Supplier<AggregationMaskBuilder> compiledMaskBuilderSupplier = () -> {
            try {
                return generateAggregationMaskBuilder(1).newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        };
        return new Object[][] {{compiledMaskBuilderSupplier}, {interpretedMaskBuilderSupplier}};
    }

    @Test(dataProvider = "maskBuilderSuppliers")
    public void testSupplier(Supplier<AggregationMaskBuilder> maskBuilderSupplier)
    {
        // each builder produced from a supplier could be completely independent
        assertThat(maskBuilderSupplier.get()).isNotSameAs(maskBuilderSupplier.get());

        Page page = buildSingleColumnPage(5);
        assertThat(maskBuilderSupplier.get().buildAggregationMask(page, Optional.empty()))
                .isNotSameAs(maskBuilderSupplier.get().buildAggregationMask(page, Optional.empty()));

        boolean[] nullFlags = new boolean[5];
        nullFlags[1] = true;
        nullFlags[3] = true;
        Page pageWithNulls = buildSingleColumnPage(nullFlags);
        assertThat(maskBuilderSupplier.get().buildAggregationMask(pageWithNulls, Optional.empty()))
                .isNotSameAs(maskBuilderSupplier.get().buildAggregationMask(pageWithNulls, Optional.empty()));
        assertThat(maskBuilderSupplier.get().buildAggregationMask(pageWithNulls, Optional.empty()).getSelectedPositions())
                .isNotSameAs(maskBuilderSupplier.get().buildAggregationMask(pageWithNulls, Optional.empty()).getSelectedPositions());
        assertThat(maskBuilderSupplier.get().buildAggregationMask(pageWithNulls, Optional.empty()).getSelectedPositions())
                .isEqualTo(maskBuilderSupplier.get().buildAggregationMask(pageWithNulls, Optional.empty()).getSelectedPositions());

        // a single mask builder is allowed to share arrays across builds
        AggregationMaskBuilder maskBuilder = maskBuilderSupplier.get();
        assertThat(maskBuilder.buildAggregationMask(pageWithNulls, Optional.empty()).getSelectedPositions())
                .isSameAs(maskBuilder.buildAggregationMask(pageWithNulls, Optional.empty()).getSelectedPositions());
    }

    @Test(dataProvider = "maskBuilderSuppliers")
    public void testUnsetNulls(Supplier<AggregationMaskBuilder> maskBuilderSupplier)
    {
        AggregationMaskBuilder maskBuilder = maskBuilderSupplier.get();
        AggregationMask aggregationMask = maskBuilder.buildAggregationMask(buildSingleColumnPage(0), Optional.empty());
        assertAggregationMaskAll(aggregationMask, 0);

        for (int positionCount = 7; positionCount < 10; positionCount++) {
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPageRle(positionCount, Optional.of(true)), Optional.empty()), positionCount);

            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.empty()), positionCount);

            boolean[] nullFlags = new boolean[positionCount];
            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPage(nullFlags), Optional.empty()), positionCount);

            Arrays.fill(nullFlags, true);
            nullFlags[1] = false;
            nullFlags[3] = false;
            nullFlags[5] = false;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(nullFlags), Optional.empty()), positionCount, 1, 3, 5);

            nullFlags[3] = true;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(nullFlags), Optional.empty()), positionCount, 1, 5);

            nullFlags[2] = false;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(nullFlags), Optional.empty()), positionCount, 1, 2, 5);

            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPageRle(positionCount, Optional.empty()), Optional.empty()), positionCount);
            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPageRle(positionCount, Optional.of(false)), Optional.empty()), positionCount);
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPageRle(positionCount, Optional.of(true)), Optional.empty()), positionCount);
        }
    }

    @Test(dataProvider = "maskBuilderSuppliers")
    public void testApplyMask(Supplier<AggregationMaskBuilder> maskBuilderSupplier)
    {
        AggregationMaskBuilder maskBuilder = maskBuilderSupplier.get();

        for (int positionCount = 7; positionCount < 10; positionCount++) {
            byte[] mask = new byte[positionCount];
            Arrays.fill(mask, (byte) 1);

            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlock(positionCount, mask))), positionCount);

            Arrays.fill(mask, (byte) 0);
            mask[1] = 1;
            mask[3] = 1;
            mask[5] = 1;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlock(positionCount, mask))), positionCount, 1, 3, 5);

            mask[3] = 0;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlock(positionCount, mask))), positionCount, 1, 5);

            mask[2] = 1;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlock(positionCount, mask))), positionCount, 1, 2, 5);

            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlockRle(positionCount, (byte) 1))), positionCount);
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlockRle(positionCount, (byte) 0))), positionCount);
        }
    }

    @Test(dataProvider = "maskBuilderSuppliers")
    public void testApplyMaskNulls(Supplier<AggregationMaskBuilder> maskBuilderSupplier)
    {
        AggregationMaskBuilder maskBuilder = maskBuilderSupplier.get();

        for (int positionCount = 7; positionCount < 10; positionCount++) {
            byte[] mask = new byte[positionCount];
            Arrays.fill(mask, (byte) 1);

            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlock(positionCount, mask))), positionCount);

            boolean[] nullFlags = new boolean[positionCount];
            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlockNulls(nullFlags))), positionCount);

            Arrays.fill(nullFlags, true);
            nullFlags[1] = false;
            nullFlags[3] = false;
            nullFlags[5] = false;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlockNulls(nullFlags))), positionCount, 1, 3, 5);

            nullFlags[3] = true;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlockNulls(nullFlags))), positionCount, 1, 5);

            nullFlags[1] = true;
            nullFlags[5] = true;
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlockNulls(nullFlags))), positionCount);

            assertAggregationMaskAll(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlockNullsRle(positionCount, false))), positionCount);
            assertAggregationMaskPositions(maskBuilder.buildAggregationMask(buildSingleColumnPage(positionCount), Optional.of(createMaskBlockNullsRle(positionCount, true))), positionCount);
        }
    }

    private static Block createMaskBlock(int positionCount, byte[] mask)
    {
        return new ByteArrayBlock(positionCount, Optional.empty(), mask);
    }

    private static Block createMaskBlockRle(int positionCount, byte mask)
    {
        return RunLengthEncodedBlock.create(createMaskBlock(1, new byte[] {mask}), positionCount);
    }

    private static Block createMaskBlockNulls(boolean[] nulls)
    {
        int positionCount = nulls.length;
        byte[] mask = new byte[positionCount];
        Arrays.fill(mask, (byte) 1);
        return new ByteArrayBlock(positionCount, Optional.of(nulls), mask);
    }

    private static Block createMaskBlockNullsRle(int positionCount, boolean nullValue)
    {
        return RunLengthEncodedBlock.create(createMaskBlockNulls(new boolean[] {nullValue}), positionCount);
    }

    private static Page buildSingleColumnPage(int positionCount)
    {
        boolean[] ignoredColumnNulls = new boolean[positionCount];
        Arrays.fill(ignoredColumnNulls, true);
        return new Page(
                new ShortArrayBlock(positionCount, Optional.of(ignoredColumnNulls), new short[positionCount]),
                new IntArrayBlock(positionCount, Optional.empty(), new int[positionCount]));
    }

    private static Page buildSingleColumnPage(boolean[] nulls)
    {
        int positionCount = nulls.length;
        boolean[] ignoredColumnNulls = new boolean[positionCount];
        Arrays.fill(ignoredColumnNulls, true);
        return new Page(
                new ShortArrayBlock(positionCount, Optional.of(ignoredColumnNulls), new short[positionCount]),
                new IntArrayBlock(positionCount, Optional.of(nulls), new int[positionCount]));
    }

    private static Page buildSingleColumnPageRle(int positionCount, Optional<Boolean> nullValue)
    {
        Optional<boolean[]> nulls = nullValue.map(value -> new boolean[] {value});
        boolean[] ignoredColumnNulls = new boolean[positionCount];
        Arrays.fill(ignoredColumnNulls, true);
        return new Page(
                new ShortArrayBlock(positionCount, Optional.of(ignoredColumnNulls), new short[positionCount]),
                RunLengthEncodedBlock.create(new IntArrayBlock(1, nulls, new int[positionCount]), positionCount));
    }

    private static void assertAggregationMaskAll(AggregationMask aggregationMask, int expectedPositionCount)
    {
        assertThat(aggregationMask.isSelectAll()).isTrue();
        assertThat(aggregationMask.isSelectNone()).isEqualTo(expectedPositionCount == 0);
        assertThat(aggregationMask.getPositionCount()).isEqualTo(expectedPositionCount);
        assertThat(aggregationMask.getSelectedPositionCount()).isEqualTo(expectedPositionCount);
        assertThatThrownBy(aggregationMask::getSelectedPositions).isInstanceOf(IllegalStateException.class);
    }

    private static void assertAggregationMaskPositions(AggregationMask aggregationMask, int expectedPositionCount, int... expectedPositions)
    {
        assertThat(aggregationMask.isSelectAll()).isFalse();
        assertThat(aggregationMask.isSelectNone()).isEqualTo(expectedPositions.length == 0);
        assertThat(aggregationMask.getPositionCount()).isEqualTo(expectedPositionCount);
        assertThat(aggregationMask.getSelectedPositionCount()).isEqualTo(expectedPositions.length);
        // AssertJ is buggy and does not allow starts with to contain an empty array
        if (expectedPositions.length > 0) {
            assertThat(aggregationMask.getSelectedPositions()).startsWith(expectedPositions);
        }
    }
}
