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
import io.trino.json.Json;
import io.trino.json.JsonBlock;
import io.trino.json.JsonBlockBuilder;
import io.trino.json.JsonItemBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInternalBlockEncodingSerde
{
    private final Map<String, BlockEncoding> blockEncodings = ImmutableMap.of(VariableWidthBlockEncoding.NAME, new VariableWidthBlockEncoding());
    private final Map<Class<? extends Block>, BlockEncoding> blockNames = ImmutableMap.of(VariableWidthBlock.class, new VariableWidthBlockEncoding());
    private final BlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodings::get, blockNames::get, TESTING_TYPE_MANAGER::getType);

    @Test
    public void blockRoundTrip()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 2);
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("hello"));
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("world"));

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, blockBuilder.build());
        Block copy = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertThat(VARCHAR.getSlice(copy, 0).toStringUtf8()).isEqualTo("hello");
        assertThat(VARCHAR.getSlice(copy, 1).toStringUtf8()).isEqualTo("world");
    }

    @Test
    public void testRegisteredJsonEncoding()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 4);
        builder.appendJson(JsonItemBuilder.encodeBigint(7));
        builder.appendJson(Json.unchecked(Slices.utf8Slice(" {\"b\":1,\"a\":2} ")));
        builder.appendNull();
        builder.appendJson(JsonItemBuilder.encodeBoolean(true));
        JsonBlock source = builder.buildValueBlock().getRegion(1, 3);
        DynamicSliceOutput output = new DynamicSliceOutput(128);
        TESTING_BLOCK_ENCODING_SERDE.writeBlock(output, source);
        JsonBlock restored = (JsonBlock) TESTING_BLOCK_ENCODING_SERDE.readBlock(output.slice().getInput());
        assertThat(restored.getPositionCount()).isEqualTo(3);
        assertThat(restored.getJson(0).isRawText()).isTrue();
        assertThat(restored.getJson(0).rawText()).isEqualTo(source.getJson(0).rawText());
        assertThat(restored.isNull(1)).isTrue();
        assertThat(restored.getJson(2)).isEqualTo(source.getJson(2));
    }

    @Test
    public void testTypeRoundTrip()
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeType(sliceOutput, BOOLEAN);
        Type actualType = blockEncodingSerde.readType(sliceOutput.slice().getInput());
        assertThat(actualType).isEqualTo(BOOLEAN);
    }
}
