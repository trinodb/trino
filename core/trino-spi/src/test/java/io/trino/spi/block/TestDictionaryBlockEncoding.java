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
import org.testng.annotations.Test;

import static io.trino.spi.block.BlockTestUtils.assertBlockEquals;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDictionaryBlockEncoding
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
    private final Block dictionary = buildTestDictionary();

    @Test
    public void testRoundTrip()
    {
        int positionCount = 40;

        // build ids
        int[] ids = new int[positionCount];
        for (int i = 0; i < 40; i++) {
            ids[i] = i % 4;
        }

        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, ids);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertTrue(actualBlock instanceof DictionaryBlock);
        DictionaryBlock actualDictionaryBlock = (DictionaryBlock) actualBlock;
        assertBlockEquals(VARCHAR, actualDictionaryBlock.getDictionary(), dictionary);
        for (int position = 0; position < actualDictionaryBlock.getPositionCount(); position++) {
            assertEquals(actualDictionaryBlock.getId(position), ids[position]);
        }
        assertEquals(actualDictionaryBlock.getDictionarySourceId(), dictionaryBlock.getDictionarySourceId());
    }

    @Test
    public void testNonSequentialDictionaryUnnest()
    {
        int[] ids = new int[] {3, 2, 1, 0};
        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, ids);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertTrue(actualBlock instanceof DictionaryBlock);
        assertBlockEquals(VARCHAR, actualBlock, dictionary.getPositions(ids, 0, 4));
    }

    @Test
    public void testNonSequentialDictionaryUnnestWithGaps()
    {
        int[] ids = new int[] {3, 2, 0};
        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, ids);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertTrue(actualBlock instanceof VariableWidthBlock);
        assertBlockEquals(VARCHAR, actualBlock, dictionary.getPositions(ids, 0, 3));
    }

    @Test
    public void testSequentialDictionaryUnnest()
    {
        int[] ids = new int[] {0, 1, 2, 3};
        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, ids);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertTrue(actualBlock instanceof VariableWidthBlock);
        assertBlockEquals(VARCHAR, actualBlock, dictionary.getPositions(ids, 0, 4));
    }

    @Test
    public void testNestedSequentialDictionaryUnnest()
    {
        int[] ids = new int[] {0, 1, 2, 3};
        DictionaryBlock nestedDictionaryBlock = new DictionaryBlock(dictionary, ids);
        DictionaryBlock dictionary = new DictionaryBlock(nestedDictionaryBlock, ids);

        Block actualBlock = roundTripBlock(dictionary);
        assertTrue(actualBlock instanceof VariableWidthBlock);
        assertBlockEquals(VARCHAR, actualBlock, this.dictionary.getPositions(ids, 0, 4));
    }

    private Block roundTripBlock(Block block)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, block);
        return blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
    }

    private Block buildTestDictionary()
    {
        // build dictionary
        BlockBuilder dictionaryBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(dictionaryBuilder, "alice");
        VARCHAR.writeString(dictionaryBuilder, "bob");
        VARCHAR.writeString(dictionaryBuilder, "charlie");
        VARCHAR.writeString(dictionaryBuilder, "dave");
        return dictionaryBuilder.build();
    }
}
