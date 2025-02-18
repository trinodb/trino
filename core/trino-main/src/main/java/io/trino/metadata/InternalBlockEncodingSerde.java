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

import com.google.inject.Inject;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import org.assertj.core.util.VisibleForTesting;

import java.util.Optional;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class InternalBlockEncodingSerde
        implements BlockEncodingSerde
{
    private final Function<String, BlockEncoding> nameToEncoding; // for deserialization
    private final Function<Class<? extends Block>, BlockEncoding> blockToEncoding; // for serialization
    private final Function<TypeId, Type> types;

    @Inject
    public InternalBlockEncodingSerde(BlockEncodingManager blockEncodingManager, TypeManager typeManager)
    {
        this(blockEncodingManager::getBlockEncodingByName, blockEncodingManager::getBlockEncodingByBlockClass, typeManager::getType);
    }

    @VisibleForTesting
    InternalBlockEncodingSerde(Function<String, BlockEncoding> nameToEncoding, Function<Class<? extends Block>, BlockEncoding> blockToEncoding, Function<TypeId, Type> types)
    {
        this.nameToEncoding = requireNonNull(nameToEncoding, "nameToEncoding is null");
        this.blockToEncoding = requireNonNull(blockToEncoding, "blockToEncoding is null");
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public Block readBlock(SliceInput input)
    {
        // read the encoding name
        String encodingName = readLengthPrefixedString(input);

        // look up the encoding factory
        BlockEncoding blockEncoding = nameToEncoding.apply(encodingName);

        // load read the encoding factory from the output stream
        return blockEncoding.readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block block)
    {
        while (true) {
            // look up the BlockEncoding
            BlockEncoding blockEncoding = blockToEncoding.apply(block.getClass());

            // see if a replacement block should be written instead
            Optional<Block> replacementBlock = blockEncoding.replacementBlockForWrite(block);
            if (replacementBlock.isPresent()) {
                block = replacementBlock.get();
                continue;
            }

            // write the name to the output
            writeLengthPrefixedString(output, blockEncoding.getName());

            // write the block to the output
            blockEncoding.writeBlock(this, output, block);

            break;
        }
    }

    @Override
    public long estimatedWriteSize(Block block)
    {
        while (true) {
            BlockEncoding blockEncoding = blockToEncoding.apply(block.getClass());
            // see if a replacement block should be written instead
            Optional<Block> replacementBlock = blockEncoding.replacementBlockForWrite(block);
            if (replacementBlock.isPresent()) {
                block = replacementBlock.get();
                continue;
            }

            // length of encoding name + encoding name + block size
            // TODO: improve this estimate by adding estimatedWriteSize to BlockEncoding interface
            return SIZE_OF_INT + blockEncoding.getName().length() + block.getSizeInBytes();
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
