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

package io.trino.block;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.type.MapType;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestMapBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        testWith(createTestMap(9, 3, 4, 0, 8, 0, 6, 5));
    }

    @Test
    public void testCompactBlock()
    {
        Block emptyBlock = new ByteArrayBlock(0, Optional.empty(), new byte[0]);
        Block compactKeyBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(16).getBytes());
        Block compactValueBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(16).getBytes());
        Block inCompactKeyBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(17).getBytes());
        Block inCompactValueBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(17).getBytes());
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] mapIsNull = {false, true, false, false, false, false};

        testCompactBlock(mapType(TINYINT, TINYINT).createBlockFromKeyValue(Optional.empty(), new int[1], emptyBlock, emptyBlock));
        testCompactBlock(mapType(TINYINT, TINYINT).createBlockFromKeyValue(Optional.of(mapIsNull), offsets, compactKeyBlock, compactValueBlock));
        // TODO: Add test case for a sliced MapBlock

        // underlying key/value block is not compact
        testIncompactBlock(mapType(TINYINT, TINYINT).createBlockFromKeyValue(Optional.of(mapIsNull), offsets, inCompactKeyBlock, inCompactValueBlock));
    }

    @Test
    public void testLazyHashTableBuildOverBlockRegion()
    {
        assertLazyHashTableBuildOverBlockRegion(createTestMap(9, 3, 4, 0, 8, 0, 6, 5));
        assertLazyHashTableBuildOverBlockRegion(alternatingNullValues(createTestMap(9, 3, 4, 0, 8, 0, 6, 5)));
    }

    private void assertLazyHashTableBuildOverBlockRegion(Map<String, Long>[] testValues)
    {
        testLazyGetPositionsHashTable(testValues);
        testLazyBeginningGetRegionHashTable(testValues);
        testLazyMiddleGetRegionHashTable(testValues);
        testLazyEndGetRegionHashTable(testValues);
    }

    private void testLazyGetPositionsHashTable(Map<String, Long>[] testValues)
    {
        MapBlock block = createBlockWithValuesFromKeyValueBlock(testValues);
        assertFalse(block.isHashTablesPresent());

        int[] testPositions = {7, 1, 5, 2, 3, 7};
        Block prefix = block.getPositions(testPositions, 0, testPositions.length);

        assertFalse(block.isHashTablesPresent());

        assertBlock(prefix, getArrayPositions(testValues, testPositions));

        assertTrue(block.isHashTablesPresent());
    }

    private static Map<String, Long>[] getArrayPositions(Map<String, Long>[] testPositions, int[] positions)
    {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Map<String, Long>[] values = new Map[positions.length];
        for (int i = 0; i < positions.length; i++) {
            values[i] = testPositions[positions[i]];
        }
        return values;
    }

    private void testLazyBeginningGetRegionHashTable(Map<String, Long>[] testValues)
    {
        MapBlock block = createBlockWithValuesFromKeyValueBlock(testValues);
        assertFalse(block.isHashTablesPresent());

        MapBlock prefix = (MapBlock) block.getRegion(0, 4);

        assertFalse(block.isHashTablesPresent());
        assertFalse(prefix.isHashTablesPresent());

        assertBlock(prefix, Arrays.copyOfRange(testValues, 0, 4));

        assertTrue(block.isHashTablesPresent());
        assertTrue(prefix.isHashTablesPresent());

        MapBlock midSection = (MapBlock) block.getRegion(2, 4);
        assertTrue(midSection.isHashTablesPresent());
        assertBlock(midSection, Arrays.copyOfRange(testValues, 2, 6));

        MapBlock suffix = (MapBlock) block.getRegion(4, 4);
        assertTrue(suffix.isHashTablesPresent());
        assertBlock(suffix, Arrays.copyOfRange(testValues, 4, 8));
    }

    private void testLazyMiddleGetRegionHashTable(Map<String, Long>[] testValues)
    {
        MapBlock block = createBlockWithValuesFromKeyValueBlock(testValues);

        MapBlock midSection = (MapBlock) block.getRegion(2, 4);

        assertFalse(block.isHashTablesPresent());
        assertFalse(midSection.isHashTablesPresent());

        assertBlock(midSection, Arrays.copyOfRange(testValues, 2, 6));

        assertTrue(block.isHashTablesPresent());
        assertTrue(midSection.isHashTablesPresent());

        MapBlock prefix = (MapBlock) block.getRegion(0, 4);
        assertTrue(prefix.isHashTablesPresent());
        assertBlock(prefix, Arrays.copyOfRange(testValues, 0, 4));

        MapBlock suffix = (MapBlock) block.getRegion(4, 4);
        assertTrue(suffix.isHashTablesPresent());
        assertBlock(suffix, Arrays.copyOfRange(testValues, 4, 8));
    }

    private void testLazyEndGetRegionHashTable(Map<String, Long>[] testValues)
    {
        MapBlock block = createBlockWithValuesFromKeyValueBlock(testValues);

        MapBlock suffix = (MapBlock) block.getRegion(4, 4);

        assertFalse(block.isHashTablesPresent());
        assertFalse(suffix.isHashTablesPresent());

        assertBlock(suffix, Arrays.copyOfRange(testValues, 4, 8));

        assertTrue(block.isHashTablesPresent());
        assertTrue(suffix.isHashTablesPresent());

        MapBlock prefix = (MapBlock) block.getRegion(0, 4);
        assertTrue(prefix.isHashTablesPresent());
        assertBlock(prefix, Arrays.copyOfRange(testValues, 0, 4));

        MapBlock midSection = (MapBlock) block.getRegion(2, 4);
        assertTrue(midSection.isHashTablesPresent());
        assertBlock(midSection, Arrays.copyOfRange(testValues, 2, 6));
    }

    private static Map<String, Long>[] createTestMap(int... entryCounts)
    {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Map<String, Long>[] result = new Map[entryCounts.length];
        for (int rowNumber = 0; rowNumber < entryCounts.length; rowNumber++) {
            int entryCount = entryCounts[rowNumber];
            Map<String, Long> map = new HashMap<>();
            for (int entryNumber = 0; entryNumber < entryCount; entryNumber++) {
                map.put("key" + entryNumber, entryNumber == 5 ? null : rowNumber * 100L + entryNumber);
            }
            result[rowNumber] = map;
        }
        return result;
    }

    private void testWith(Map<String, Long>[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertFalse(blockBuilder.mayHaveNull());

        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder, 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder, 2, 3, 5, 6);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 2, 3, 5, 6);

        Block block = createBlockWithValuesFromKeyValueBlock(expectedValues);
        assertFalse(block.mayHaveNull());

        assertBlock(block, expectedValues);
        assertBlockFilteredPositions(expectedValues, block, 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, block, 2, 3, 5, 6);

        Map<String, Long>[] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertTrue(blockBuilderWithNull.mayHaveNull());

        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull, 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull, 2, 3, 4, 9, 13, 14);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 2, 3, 4, 9, 13, 14);

        Block blockWithNull = createBlockWithValuesFromKeyValueBlock(expectedValuesWithNull);
        assertTrue(blockWithNull.mayHaveNull());

        assertBlock(blockWithNull, expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 2, 3, 4, 9, 13, 14);
    }

    private BlockBuilder createBlockBuilderWithValues(Map<String, Long>[] maps)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        for (Map<String, Long> map : maps) {
            createBlockBuilderWithValues(map, mapBlockBuilder);
        }
        return mapBlockBuilder;
    }

    private MapBlock createBlockWithValuesFromKeyValueBlock(Map<String, Long>[] maps)
    {
        List<String> keys = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        int[] offsets = new int[maps.length + 1];
        boolean[] mapIsNull = new boolean[maps.length];
        boolean hasNullValue = false;
        for (int i = 0; i < maps.length; i++) {
            Map<String, Long> map = maps[i];
            boolean isNull = map == null;
            mapIsNull[i] = isNull;
            hasNullValue |= isNull;
            if (map == null) {
                offsets[i + 1] = offsets[i];
            }
            else {
                for (Map.Entry<String, Long> entry : map.entrySet()) {
                    keys.add(entry.getKey());
                    values.add(entry.getValue());
                }
                offsets[i + 1] = offsets[i] + map.size();
            }
        }
        return (MapBlock) mapType(VARCHAR, BIGINT).createBlockFromKeyValue(
                hasNullValue ? Optional.of(mapIsNull) : Optional.empty(),
                offsets,
                createStringsBlock(keys),
                createLongsBlock(values));
    }

    private void createBlockBuilderWithValues(Map<String, Long> map, BlockBuilder mapBlockBuilder)
    {
        if (map == null) {
            mapBlockBuilder.appendNull();
        }
        else {
            BlockBuilder elementBlockBuilder = mapBlockBuilder.beginBlockEntry();
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                VARCHAR.writeSlice(elementBlockBuilder, utf8Slice(entry.getKey()));
                if (entry.getValue() == null) {
                    elementBlockBuilder.appendNull();
                }
                else {
                    BIGINT.writeLong(elementBlockBuilder, entry.getValue());
                }
            }
            mapBlockBuilder.closeEntry();
        }
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue instanceof Map) {
            assertValue(block, position, (Map<String, Long>) expectedValue);
            return;
        }
        super.assertPositionValue(block, position, expectedValue);
    }

    private void assertValue(Block mapBlock, int position, Map<String, Long> map)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);

        // null maps are handled by assertPositionValue
        requireNonNull(map, "map is null");

        assertFalse(mapBlock.isNull(position));
        SingleMapBlock elementBlock = (SingleMapBlock) mapType.getObject(mapBlock, position);
        assertEquals(elementBlock.getPositionCount(), map.size() * 2);

        // Test new/hash-index access: assert inserted keys
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            int pos = elementBlock.seekKey(utf8Slice(entry.getKey()));
            assertNotEquals(pos, -1);
            if (entry.getValue() == null) {
                assertTrue(elementBlock.isNull(pos));
            }
            else {
                assertFalse(elementBlock.isNull(pos));
                assertEquals(BIGINT.getLong(elementBlock, pos), (long) entry.getValue());
            }
        }
        // Test new/hash-index access: assert non-existent keys
        for (int i = 0; i < 10; i++) {
            assertEquals(elementBlock.seekKey(utf8Slice("not-inserted-" + i)), -1);
        }

        // Test legacy/iterative access
        for (int i = 0; i < elementBlock.getPositionCount(); i += 2) {
            String actualKey = VARCHAR.getSlice(elementBlock, i).toStringUtf8();
            Long actualValue;
            if (elementBlock.isNull(i + 1)) {
                actualValue = null;
            }
            else {
                actualValue = BIGINT.getLong(elementBlock, i + 1);
            }
            assertTrue(map.containsKey(actualKey));
            assertEquals(actualValue, map.get(actualKey));
        }
    }

    @Test
    public void testStrict()
    {
        MapType mapType = mapType(BIGINT, BIGINT);
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, 1);
        mapBlockBuilder.strict();

        // Add 100 maps with only one entry but the same key
        for (int i = 0; i < 100; i++) {
            BlockBuilder entryBuilder = mapBlockBuilder.beginBlockEntry();
            BIGINT.writeLong(entryBuilder, 1);
            BIGINT.writeLong(entryBuilder, -1);
            mapBlockBuilder.closeEntry();
        }

        BlockBuilder entryBuilder = mapBlockBuilder.beginBlockEntry();
        // Add 50 keys so we get some chance to get hash conflict
        // The purpose of this test is to make sure offset is calculated correctly in MapBlockBuilder.closeEntryStrict()
        for (int i = 0; i < 50; i++) {
            BIGINT.writeLong(entryBuilder, i);
            BIGINT.writeLong(entryBuilder, -1);
        }
        mapBlockBuilder.closeEntry();

        entryBuilder = mapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < 2; i++) {
            BIGINT.writeLong(entryBuilder, 99);
            BIGINT.writeLong(entryBuilder, -1);
        }
        assertThatThrownBy(mapBlockBuilder::closeEntry)
                .isInstanceOf(DuplicateMapKeyException.class)
                .hasMessage("Duplicate map keys are not allowed");
    }

    @Test
    public void testCloseEntryStrict()
            throws Exception
    {
        MapType mapType = mapType(BIGINT, BIGINT);
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, 1);

        // Add 100 maps with only one entry but the same key
        for (int i = 0; i < 100; i++) {
            BlockBuilder entryBuilder = mapBlockBuilder.beginBlockEntry();
            BIGINT.writeLong(entryBuilder, 1);
            BIGINT.writeLong(entryBuilder, -1);
            mapBlockBuilder.closeEntry();
        }

        BlockBuilder entryBuilder = mapBlockBuilder.beginBlockEntry();
        // Add 50 keys so we get some chance to get hash conflict
        // The purpose of this test is to make sure offset is calculated correctly in MapBlockBuilder.closeEntryStrict()
        for (int i = 0; i < 50; i++) {
            BIGINT.writeLong(entryBuilder, i);
            BIGINT.writeLong(entryBuilder, -1);
        }
        mapBlockBuilder.closeEntryStrict();
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        Map<String, Long>[] expectedValues = alternatingNullValues(createTestMap(9, 3, 4, 0, 8, 0, 6, 5));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        Block block = blockBuilder.build();
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = getExpectedEstimatedDataSize(expectedValues[i]);
            assertEquals(blockBuilder.getEstimatedDataSizeForStats(i), expectedSize);
            assertEquals(block.getEstimatedDataSizeForStats(i), expectedSize);
        }
    }

    private static int getExpectedEstimatedDataSize(Map<String, Long> map)
    {
        if (map == null) {
            return 0;
        }
        int size = 0;
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            size += entry.getKey().length();
            size += entry.getValue() == null ? 0 : Long.BYTES;
        }
        return size;
    }
}
