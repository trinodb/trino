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
package io.trino.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestInternalBlockEncodingSerde
{
    private final TestingTypeManager testingTypeManager = new TestingTypeManager();
    private final Map<String, BlockEncoding> blockEncodings = ImmutableMap.of(VariableWidthBlockEncoding.NAME, new VariableWidthBlockEncoding());
    private final BlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodings::get, testingTypeManager::getType);

    @Test
    public void blockRoundTrip()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 2);
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("hello"));
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("world"));

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, blockBuilder.build());
        Block copy = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertEquals(VARCHAR.getSlice(copy, 0).toStringUtf8(), "hello");
        assertEquals(VARCHAR.getSlice(copy, 1).toStringUtf8(), "world");
    }

    @Test
    public void testTypeRoundTrip()
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeType(sliceOutput, BOOLEAN);
        Type actualType = blockEncodingSerde.readType(sliceOutput.slice().getInput());
        assertEquals(actualType, BOOLEAN);
    }
}
