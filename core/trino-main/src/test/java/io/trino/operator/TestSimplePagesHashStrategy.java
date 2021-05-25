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
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSimplePagesHashStrategy
{
    @Test
    public void testHashRowWithIntegerType()
    {
        Block block = new IntArrayBlock(1, Optional.empty(), new int[]{1234});
        SimplePagesHashStrategy strategy = createSimplePagesHashStrategy(INTEGER, ImmutableList.of(block));
        Page page = new Page(block);

        // This works because IntegerType is comparable.
        assertEquals(strategy.hashRow(0, page), -4467490526933615037L);
    }

    @Test
    public void testHashRowWithMapType()
    {
        MapType mapType = new MapType(INTEGER, INTEGER, new TypeOperators());
        Block block = mapType.createBlockFromKeyValue(
                Optional.empty(),
                new int[]{0, 1},
                new IntArrayBlock(1, Optional.empty(), new int[]{1234}),
                new IntArrayBlock(1, Optional.empty(), new int[]{5678}));

        SimplePagesHashStrategy strategy = createSimplePagesHashStrategy(mapType, ImmutableList.of(block));
        Page page = new Page(block);

        // This works because MapType is comparable.
        assertEquals(strategy.hashRow(0, page), 451258269207618863L);
    }

    @Test
    public void testRowEqualsRowWithIntegerType()
    {
        SimplePagesHashStrategy strategy = createSimplePagesHashStrategy(INTEGER, ImmutableList.of());

        Page leftPage = new Page(new IntArrayBlock(1, Optional.empty(), new int[]{1234}));
        Page rightPage1 = new Page(new IntArrayBlock(1, Optional.empty(), new int[]{1234}));
        Page rightPage2 = new Page(new IntArrayBlock(1, Optional.empty(), new int[]{5678}));

        // This works because IntegerType is comparable.
        assertTrue(strategy.rowEqualsRow(0, leftPage, 0, rightPage1));
        assertFalse(strategy.rowEqualsRow(0, leftPage, 0, rightPage2));
    }

    @Test
    public void testRowEqualsRowWithMapType()
    {
        MapType mapType = new MapType(INTEGER, INTEGER, new TypeOperators());
        SimplePagesHashStrategy strategy = createSimplePagesHashStrategy(mapType, ImmutableList.of());

        Page leftPage = new Page(mapType.createBlockFromKeyValue(
                Optional.empty(),
                new int[]{0, 1},
                new IntArrayBlock(1, Optional.empty(), new int[]{1234}),
                new IntArrayBlock(1, Optional.empty(), new int[]{5678})));

        Page rightPage1 = new Page(mapType.createBlockFromKeyValue(
                Optional.empty(),
                new int[]{0, 1},
                new IntArrayBlock(1, Optional.empty(), new int[]{1234}),
                new IntArrayBlock(1, Optional.empty(), new int[]{5678})));

        Page rightPage2 = new Page(mapType.createBlockFromKeyValue(
                Optional.empty(),
                new int[]{0, 1},
                new IntArrayBlock(1, Optional.empty(), new int[]{1234}),
                new IntArrayBlock(1, Optional.empty(), new int[]{1234})));

        // This works because MapType is comparable.
        assertTrue(strategy.rowEqualsRow(0, leftPage, 0, rightPage1));
        assertFalse(strategy.rowEqualsRow(0, leftPage, 0, rightPage2));
    }

    @Test
    public void testCompareSortChannelPositionsWithIntegerType()
    {
        Block block = new IntArrayBlock(3, Optional.empty(), new int[]{1234, 5678, 1234});
        SimplePagesHashStrategy strategy = createSimplePagesHashStrategy(INTEGER, ImmutableList.of(block));

        // This works because IntegerType is orderable.
        assertEquals(strategy.compareSortChannelPositions(0, 0, 0, 1), -1);
        assertEquals(strategy.compareSortChannelPositions(0, 1, 0, 0), 1);
        assertEquals(strategy.compareSortChannelPositions(0, 0, 0, 2), 0);
    }

    @Test
    public void testCompareSortChannelPositionsWithMapType()
    {
        MapType mapType = new MapType(INTEGER, INTEGER, new TypeOperators());
        Block block = mapType.createBlockFromKeyValue(
                Optional.empty(),
                new int[]{0, 1},
                new IntArrayBlock(1, Optional.empty(), new int[]{1234}),
                new IntArrayBlock(1, Optional.empty(), new int[]{5678}));

        SimplePagesHashStrategy strategy = createSimplePagesHashStrategy(mapType, ImmutableList.of(block));

        // This fails because MapType is not orderable.
        assertThatThrownBy(() -> strategy.compareSortChannelPositions(0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type is not orderable");
    }

    private static SimplePagesHashStrategy createSimplePagesHashStrategy(Type type, List<Block> channelBlocks)
    {
        return new SimplePagesHashStrategy(
                ImmutableList.of(type),
                ImmutableList.of(),
                ImmutableList.of(channelBlocks),
                ImmutableList.of(0),
                OptionalInt.empty(),
                Optional.of(0),
                new BlockTypeOperators());
    }
}
