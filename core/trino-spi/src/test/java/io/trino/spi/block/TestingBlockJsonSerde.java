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

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;

import java.util.Base64;

import static java.util.Objects.requireNonNull;

public final class TestingBlockJsonSerde
{
    private TestingBlockJsonSerde() {}

    public static class Serializer
            extends ValueSerializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        public Serializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public void serialize(Block block, JsonGenerator jsonGenerator, SerializationContext context)
        {
            SliceOutput output = new DynamicSliceOutput(64);
            blockEncodingSerde.writeBlock(output, block);
            String encoded = Base64.getEncoder().encodeToString(output.slice().getBytes());
            jsonGenerator.writeString(encoded);
        }
    }

    public static class Deserializer
            extends ValueDeserializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        public Deserializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public Block deserialize(JsonParser jsonParser, DeserializationContext context)
        {
            byte[] decoded = Base64.getDecoder().decode(jsonParser.readValueAs(String.class));
            BasicSliceInput input = Slices.wrappedBuffer(decoded).getInput();
            return blockEncodingSerde.readBlock(input);
        }
    }
}
