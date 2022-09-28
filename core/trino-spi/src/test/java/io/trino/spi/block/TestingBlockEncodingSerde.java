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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

// This class is exactly the same as BlockEncodingManager. They are in SPI and don't have access to InternalBlockEncodingSerde.
public final class TestingBlockEncodingSerde
        implements BlockEncodingSerde
{
    private final Function<TypeId, Type> types;
    private final ConcurrentMap<String, BlockEncoding> blockEncodings = new ConcurrentHashMap<>();

    public TestingBlockEncodingSerde()
    {
        this(new TestingTypeManager()::getType);
    }

    public TestingBlockEncodingSerde(Function<TypeId, Type> types)
    {
        this.types = requireNonNull(types, "types is null");
        // add the built-in BlockEncodings
        addBlockEncoding(new VariableWidthBlockEncoding());
        addBlockEncoding(new ByteArrayBlockEncoding());
        addBlockEncoding(new ShortArrayBlockEncoding());
        addBlockEncoding(new IntArrayBlockEncoding());
        addBlockEncoding(new LongArrayBlockEncoding());
        addBlockEncoding(new Int96ArrayBlockEncoding());
        addBlockEncoding(new Int128ArrayBlockEncoding());
        addBlockEncoding(new DictionaryBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding());
        addBlockEncoding(new MapBlockEncoding());
        addBlockEncoding(new SingleMapBlockEncoding());
        addBlockEncoding(new RowBlockEncoding());
        addBlockEncoding(new SingleRowBlockEncoding());
        addBlockEncoding(new RunLengthBlockEncoding());
        addBlockEncoding(new LazyBlockEncoding());
    }

    private void addBlockEncoding(BlockEncoding blockEncoding)
    {
        blockEncodings.put(blockEncoding.getName(), blockEncoding);
    }

    @Override
    public Block readBlock(SliceInput input)
    {
        // read the encoding name
        String encodingName = readLengthPrefixedString(input);

        // look up the encoding factory
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding %s", encodingName);

        // load read the encoding factory from the output stream
        return blockEncoding.readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block block)
    {
        while (true) {
            // get the encoding name
            String encodingName = block.getEncodingName();

            // look up the encoding factory
            BlockEncoding blockEncoding = blockEncodings.get(encodingName);

            // see if a replacement block should be written instead
            Optional<Block> replacementBlock = blockEncoding.replacementBlockForWrite(block);
            if (replacementBlock.isPresent()) {
                block = replacementBlock.get();
                continue;
            }

            // write the name to the output
            writeLengthPrefixedString(output, encodingName);

            // write the block to the output
            blockEncoding.writeBlock(this, output, block);

            break;
        }
    }

    @Override
    public Type readType(SliceInput sliceInput)
    {
        requireNonNull(sliceInput, "sliceInput is null");

        String id = readLengthPrefixedString(sliceInput);
        Type type = types.apply(TypeId.of(id));
        if (type == null) {
            throw new IllegalArgumentException("Unknown type " + id);
        }
        return type;
    }

    @Override
    public void writeType(SliceOutput sliceOutput, Type type)
    {
        requireNonNull(sliceOutput, "sliceOutput is null");
        requireNonNull(type, "type is null");
        writeLengthPrefixedString(sliceOutput, type.getTypeId().getId());
    }

    private static String readLengthPrefixedString(SliceInput input)
    {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        return new String(bytes, UTF_8);
    }

    private static void writeLengthPrefixedString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
