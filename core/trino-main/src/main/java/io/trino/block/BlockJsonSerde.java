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
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.Base64;

import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.block.BlockSerdeUtil.readBlock;
import static io.trino.block.BlockSerdeUtil.writeBlock;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public final class BlockJsonSerde
{
    // 3 KB of bytes encode to exactly 4 KB of base64 text
    private static final int DECODED_CHUNK_SIZE = 3 * 1024;
    private static final int ENCODED_CHUNK_SIZE = 4 * 1024;

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
            DynamicSliceOutput base64 = new DynamicSliceOutput(toIntExact((blockEncodingSerde.estimatedWriteSize(block) + 2) / 3 * 4));
            try (SliceOutput output = new OutputStreamSliceOutput(new Base64EncodingOutputStream(base64))) {
                writeBlock(blockEncodingSerde, output, block);
            }
            Slice text = base64.slice();
            // base64 text needs no JSON escaping, so a UTF-8 generator copies it as is
            if (jsonGenerator instanceof UTF8JsonGenerator) {
                jsonGenerator.writeRawUTF8String(text.byteArray(), text.byteArrayOffset(), text.length());
            }
            else {
                jsonGenerator.writeString(text.toStringAscii());
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
            Base64DecodingWriter text = new Base64DecodingWriter(parser.getTextLength());
            parser.getText(text);
            return readBlock(blockEncodingSerde, text.decoded());
        }
    }

    /**
     * Encodes bytes to base64 in fixed size chunks, since the JDK codec encodes whole arrays in bulk.
     */
    private static final class Base64EncodingOutputStream
            extends OutputStream
    {
        private final SliceOutput output;
        private final byte[] chunk = new byte[DECODED_CHUNK_SIZE];
        private final byte[] encodedChunk = new byte[ENCODED_CHUNK_SIZE];
        private int chunkSize;

        private Base64EncodingOutputStream(SliceOutput output)
        {
            this.output = requireNonNull(output, "output is null");
        }

        @Override
        public void write(int value)
        {
            chunk[chunkSize++] = (byte) value;
            if (chunkSize == chunk.length) {
                encode(chunk);
            }
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
        {
            checkFromIndexSize(offset, length, bytes.length);
            while (length > 0) {
                int count = min(length, chunk.length - chunkSize);
                System.arraycopy(bytes, offset, chunk, chunkSize, count);
                chunkSize += count;
                offset += count;
                length -= count;
                if (chunkSize == chunk.length) {
                    encode(chunk);
                }
            }
        }

        @Override
        public void close()
        {
            if (chunkSize > 0) {
                encode(Arrays.copyOf(chunk, chunkSize));
            }
        }

        private void encode(byte[] source)
        {
            int length = Base64.getEncoder().encode(source, encodedChunk);
            output.writeBytes(encodedChunk, 0, length);
            chunkSize = 0;
        }
    }

    /**
     * Decodes base64 text in fixed size chunks, since the JDK codec decodes whole arrays in bulk.
     */
    private static final class Base64DecodingWriter
            extends Writer
    {
        private final byte[] decoded;
        private final byte[] chunk = new byte[ENCODED_CHUNK_SIZE];
        private final byte[] decodedChunk = new byte[DECODED_CHUNK_SIZE];
        private int remaining;
        private int chunkSize;
        private int decodedSize;

        private Base64DecodingWriter(int length)
        {
            this.remaining = length;
            this.decoded = new byte[toIntExact((length + 3L) / 4 * 3)];
        }

        @Override
        public void write(char[] buffer, int offset, int length)
        {
            checkFromIndexSize(offset, length, buffer.length);
            checkArgument(length <= remaining, "Base64 text is longer than its declared length");
            remaining -= length;
            while (length > 0) {
                int count = min(length, chunk.length - chunkSize);
                int bits = 0;
                for (int i = 0; i < count; i++) {
                    char value = buffer[offset + i];
                    bits |= value;
                    chunk[chunkSize + i] = (byte) value;
                }
                checkArgument(bits <= 0x7F, "Illegal base64 character");
                chunkSize += count;
                offset += count;
                length -= count;
                if (chunkSize == chunk.length && length + remaining > 0) {
                    // padding ends the text, so only the last chunk may end with it
                    checkArgument(chunk[chunk.length - 1] != '=', "Base64 padding before the end of the text");
                    decode(chunk);
                }
            }
        }

        public Slice decoded()
        {
            checkState(remaining == 0, "Base64 text is shorter than its declared length");
            if (chunkSize == chunk.length) {
                decode(chunk);
            }
            else if (chunkSize > 0) {
                decode(Arrays.copyOf(chunk, chunkSize));
            }
            return Slices.wrappedBuffer(decoded, 0, decodedSize);
        }

        private void decode(byte[] source)
        {
            int length = Base64.getDecoder().decode(source, decodedChunk);
            System.arraycopy(decodedChunk, 0, decoded, decodedSize, length);
            decodedSize += length;
            chunkSize = 0;
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
