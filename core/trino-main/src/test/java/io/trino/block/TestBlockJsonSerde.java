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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonMapperProvider;
import io.airlift.slice.DynamicSliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockSerdeUtil.writeBlock;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBlockJsonSerde
{
    private static final JsonMapper MAPPER = new JsonMapperProvider()
            .withJsonDeserializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE)))
            .withJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
            .get();

    @Test
    public void testRoundTrip()
            throws Exception
    {
        assertRoundTrip(BIGINT, createLongsBlock(1L, null, 3L));
        assertRoundTrip(VARCHAR, createStringsBlock("alice", null, "bob"));
        assertRoundTrip(BIGINT, createLongsBlock(ImmutableList.of()));
    }

    @Test
    public void testRoundTripAsCreatorProperty()
            throws Exception
    {
        Block block = createLongsBlock(1L, 2L, 3L);
        Holder holder = new Holder("name", block, 42);
        Holder deserialized = MAPPER.readValue(MAPPER.writeValueAsBytes(holder), Holder.class);
        assertThat(deserialized.name()).isEqualTo("name");
        assertThat(deserialized.count()).isEqualTo(42);
        assertBlockEquals(BIGINT, deserialized.block(), block);
    }

    @Test
    public void testRoundTripThroughTree()
            throws Exception
    {
        Block block = createStringsBlock("alice", "bob");
        Block deserialized = MAPPER.treeToValue(MAPPER.valueToTree(block), Block.class);
        assertBlockEquals(VARCHAR, deserialized, block);
    }

    @Test
    public void testJsonIsPaddedBase64OfBlockEncoding()
            throws Exception
    {
        Block block = createStringsBlock("alice", null, "bob");
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        writeBlock(TESTING_BLOCK_ENCODING_SERDE, output, block);
        String expected = "\"" + Base64Variants.MIME_NO_LINEFEEDS.encode(output.slice().getBytes()) + "\"";
        assertThat(new String(MAPPER.writeValueAsBytes(block), UTF_8)).isEqualTo(expected);
        assertThat(MAPPER.writeValueAsString(block)).isEqualTo(expected);
    }

    private static void assertRoundTrip(Type type, Block block)
            throws Exception
    {
        byte[] json = MAPPER.writeValueAsBytes(block);
        assertBlockEquals(type, MAPPER.readValue(json, Block.class), block);
        assertBlockEquals(type, MAPPER.readValue(new String(json, UTF_8), Block.class), block);
        assertBlockEquals(type, MAPPER.readValue(MAPPER.writeValueAsString(block), Block.class), block);
    }

    public record Holder(String name, Block block, int count)
    {
        @JsonCreator
        public Holder(
                @JsonProperty("name") String name,
                @JsonProperty("block") Block block,
                @JsonProperty("count") int count)
        {
            this.name = name;
            this.block = block;
            this.count = count;
        }

        @JsonProperty
        @Override
        public String name()
        {
            return name;
        }

        @JsonProperty
        @Override
        public Block block()
        {
            return block;
        }

        @JsonProperty
        @Override
        public int count()
        {
            return count;
        }
    }
}
