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

import com.google.inject.Inject;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import tools.jackson.core.Base64Variants;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;

import static io.trino.block.BlockSerdeUtil.readBlock;
import static io.trino.block.BlockSerdeUtil.writeBlock;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class BlockJsonSerde
{
    private BlockJsonSerde() {}

    public static class Serializer
            extends ValueSerializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        @Inject
        public Serializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public void serialize(Block block, JsonGenerator jsonGenerator, SerializationContext context)
        {
            SliceOutput output = new DynamicSliceOutput(toIntExact(blockEncodingSerde.estimatedWriteSize(block)));
            writeBlock(blockEncodingSerde, output, block);
            Slice slice = output.slice();
            jsonGenerator.writeBinary(Base64Variants.MIME_NO_LINEFEEDS, slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }
    }

    public static class Deserializer
            extends ValueDeserializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        @Inject
        public Deserializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public Block deserialize(JsonParser jsonParser, DeserializationContext context)
        {
            byte[] decoded = jsonParser.getBinaryValue(Base64Variants.MIME_NO_LINEFEEDS);
            return readBlock(blockEncodingSerde, Slices.wrappedBuffer(decoded));
        }
    }
}
