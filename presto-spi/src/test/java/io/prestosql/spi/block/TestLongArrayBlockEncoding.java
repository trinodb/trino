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
package io.prestosql.spi.block;

import io.airlift.slice.DynamicSliceOutput;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestLongArrayBlockEncoding
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    @Test
    public void testRoundTripNoNull()
    {
        BlockBuilder expectedBlockBuilder = BIGINT.createBlockBuilder(null, 4);
        BIGINT.writeLong(expectedBlockBuilder, 1);
        BIGINT.writeLong(expectedBlockBuilder, 2);
        BIGINT.writeLong(expectedBlockBuilder, 3);
        BIGINT.writeLong(expectedBlockBuilder, 4);
        Block expectedBlock = expectedBlockBuilder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, expectedBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(BIGINT, actualBlock, expectedBlock);
    }

    @Test
    public void testRoundTripWithNull()
    {
        BlockBuilder expectedBlockBuilder = BIGINT.createBlockBuilder(null, 7);
        expectedBlockBuilder.appendNull();
        expectedBlockBuilder.appendNull();
        BIGINT.writeLong(expectedBlockBuilder, 1);
        expectedBlockBuilder.appendNull();
        BIGINT.writeLong(expectedBlockBuilder, 3);
        expectedBlockBuilder.appendNull();
        expectedBlockBuilder.appendNull();
        Block expectedBlock = expectedBlockBuilder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, expectedBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(BIGINT, actualBlock, expectedBlock);
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }
}
