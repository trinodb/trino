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
package io.trino.spi.block;

import io.airlift.slice.DynamicSliceOutput;
import io.trino.spi.type.Type;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.spi.block.BlockTestUtils.assertBlockEquals;

public abstract class BaseBlockEncodingTest<T>
{
    private static final int[] RANDOM_BLOCK_SIZES = {2, 4, 8, 9, 16, 17, 32, 33, 64, 65, 1000, 1000000};

    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    protected abstract Type getType();

    protected abstract void write(BlockBuilder blockBuilder, T value);

    protected abstract T randomValue(Random random);

    @Test
    public void testEmpty()
    {
        roundTrip();
    }

    @Test
    public void testSingleNull()
    {
        roundTrip((T) null);
    }

    @Test
    public void testSingleValue()
    {
        roundTrip(randomValue(getRandom()));
    }

    @Test
    public void testNullAtTheBeginningAndEnd()
    {
        Random random = getRandom();
        roundTrip(null, null, randomValue(random), null, randomValue(random), null, null);
    }

    @Test
    public void testBlocksOf8()
    {
        Random random = getRandom();

        Object[] values = Stream.of(
                BlockFill.MIXED,
                BlockFill.ONLY_NULLS,
                BlockFill.MIXED,
                BlockFill.ONLY_VALUES,
                BlockFill.MIXED,
                BlockFill.ONLY_NULLS,
                BlockFill.ONLY_VALUES,
                BlockFill.MIXED)
                .map(fill -> getObjects(8, fill, random))
                .flatMap(Arrays::stream)
                .toArray();

        roundTrip(values);
    }

    @Test(dataProvider = "testRandomDataDataProvider")
    public void testRandomData(int size, BlockFill fill)
    {
        roundTrip(getObjects(size, fill, getRandom()));
    }

    private Object[] getObjects(int size, BlockFill fill, Random random)
    {
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            switch (fill) {
                case ONLY_NULLS:
                    values[i] = null;
                    break;
                case ONLY_VALUES:
                    values[i] = randomValue(random);
                    break;
                case MIXED:
                    if (random.nextBoolean()) {
                        values[i] = null;
                    }
                    else {
                        values[i] = randomValue(random);
                    }
                    break;
            }
        }
        return values;
    }

    @DataProvider
    public static Object[][] testRandomDataDataProvider()
    {
        return Arrays.stream(BlockFill.values())
                .flatMap(fill -> IntStream.of(RANDOM_BLOCK_SIZES).mapToObj(size -> new Object[] {size, fill}))
                .toArray(Object[][]::new);
    }

    protected final void roundTrip(Object... values)
    {
        BlockBuilder expectedBlockBuilder = createBlockBuilder(values.length);

        for (Object value : values) {
            if (value == null) {
                expectedBlockBuilder.appendNull();
            }
            else {
                //noinspection unchecked
                write(expectedBlockBuilder, (T) value);
            }
        }

        Block expectedBlock = expectedBlockBuilder.build();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, expectedBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(getType(), actualBlock, expectedBlock);
    }

    private BlockBuilder createBlockBuilder(int length)
    {
        return getType().createBlockBuilder(null, length);
    }

    private static Random getRandom()
    {
        return new Random(32167);
    }

    private enum BlockFill
    {
        ONLY_NULLS,
        ONLY_VALUES,
        MIXED,
        /**/
    }
}
