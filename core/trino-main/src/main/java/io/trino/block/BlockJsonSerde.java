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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.UTF8JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.inject.Inject;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static io.trino.block.BlockSerdeUtil.readBlock;
import static io.trino.block.BlockSerdeUtil.writeBlock;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

public final class BlockJsonSerde
{
    private BlockJsonSerde() {}

    public static class Serializer
            extends JsonSerializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        @Inject
        public Serializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public void serialize(Block block, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            SliceOutput output = new DynamicSliceOutput(toIntExact(blockEncodingSerde.estimatedWriteSize(block)));
            writeBlock(blockEncodingSerde, output, block);
            ByteBuffer base64 = Base64.getEncoder().encode(output.slice().toByteBuffer());
            // base64 text needs no JSON escaping, so a UTF-8 generator copies it as is
            if (jsonGenerator instanceof UTF8JsonGenerator) {
                jsonGenerator.writeRawUTF8String(base64.array(), base64.arrayOffset(), base64.remaining());
            }
            else {
                jsonGenerator.writeString(new String(base64.array(), base64.arrayOffset(), base64.remaining(), US_ASCII));
            }
        }
    }

    public static class Deserializer
            extends JsonDeserializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        @Inject
        public Deserializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public Block deserialize(JsonParser parser, DeserializationContext context)
                throws IOException
        {
            if (parser.currentToken() != VALUE_STRING) {
                return (Block) context.handleUnexpectedToken(Block.class, parser);
            }
            byte[] decoded = Base64.getDecoder().decode(parser.getText());
            return readBlock(blockEncodingSerde, Slices.wrappedBuffer(decoded));
        }
    }
}
