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
package io.trino.operator.output;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.operator.output.PositionsAppender.TypedPositionsAppender;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.Int96ArrayBlock;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VariableWidthType;
import io.trino.sql.gen.IsolatedClass;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

/**
 * Isolates the {@code PositionsAppender} class per type and block tuples.
 * Type specific {@code PositionsAppender} implementations manually inline {@code Type#appendTo} method inside the loop
 * to avoid virtual(mega-morphic) calls and force jit to inline the {@code Block} and {@code BlockBuilder} methods.
 * Ideally, {@code TypedPositionsAppender} could work instead of type specific {@code PositionsAppender}s,
 * but in practice jit falls back to virtual calls in some cases (e.g. {@link Block#isNull}).
 */
public class PositionsAppenderFactory
{
    private final NonEvictableLoadingCache<CacheKey, PositionsAppender> cache;

    public PositionsAppenderFactory()
    {
        this.cache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000),
                CacheLoader.from(key -> createAppender(key.type)));
    }

    public PositionsAppender create(Type type, Class<? extends Block> blockClass)
    {
        return cache.getUnchecked(new CacheKey(type, blockClass));
    }

    private PositionsAppender createAppender(Type type)
    {
        return Optional.ofNullable(findDedicatedAppenderClassFor(type))
                .map(this::isolateAppender)
                .orElseGet(() -> isolateTypeAppender(type));
    }

    private Class<? extends PositionsAppender> findDedicatedAppenderClassFor(Type type)
    {
        if (type instanceof FixedWidthType) {
            switch (((FixedWidthType) type).getFixedSize()) {
                case Byte.BYTES:
                    return BytePositionsAppender.class;
                case Short.BYTES:
                    return SmallintPositionsAppender.class;
                case Integer.BYTES:
                    return IntPositionsAppender.class;
                case Long.BYTES:
                    return LongPositionsAppender.class;
                case Int96ArrayBlock.INT96_BYTES:
                    return Int96PositionsAppender.class;
                case Int128ArrayBlock.INT128_BYTES:
                    return Int128PositionsAppender.class;
                default:
                    // size not supported directly, fallback to the generic appender
            }
        }
        else if (type instanceof VariableWidthType) {
            return SlicePositionsAppender.class;
        }

        return null;
    }

    private PositionsAppender isolateTypeAppender(Type type)
    {
        Class<? extends PositionsAppender> isolatedAppenderClass = isolateAppenderClass(TypedPositionsAppender.class);
        try {
            return isolatedAppenderClass.getConstructor(Type.class).newInstance(type);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private PositionsAppender isolateAppender(Class<? extends PositionsAppender> appenderClass)
    {
        Class<? extends PositionsAppender> isolatedAppenderClass = isolateAppenderClass(appenderClass);
        try {
            return isolatedAppenderClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private Class<? extends PositionsAppender> isolateAppenderClass(Class<? extends PositionsAppender> appenderClass)
    {
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(PositionsAppender.class.getClassLoader());

        Class<? extends PositionsAppender> isolatedBatchPositionsTransferClass = IsolatedClass.isolateClass(
                dynamicClassLoader,
                PositionsAppender.class,
                appenderClass);
        return isolatedBatchPositionsTransferClass;
    }

    public static class LongPositionsAppender
            implements PositionsAppender
    {
        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeLong(block.getLong(positionArray[i], 0)).closeEntry();
                }
            }
        }
    }

    public static class IntPositionsAppender
            implements PositionsAppender
    {
        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeInt(block.getInt(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeInt(block.getInt(positionArray[i], 0)).closeEntry();
                }
            }
        }
    }

    public static class BytePositionsAppender
            implements PositionsAppender
    {
        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeByte(block.getByte(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeByte(block.getByte(positionArray[i], 0)).closeEntry();
                }
            }
        }
    }

    public static class SlicePositionsAppender
            implements PositionsAppender
    {
        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
                    blockBuilder.closeEntry();
                }
            }
        }
    }

    public static class SmallintPositionsAppender
            implements PositionsAppender
    {
        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeShort(block.getShort(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeShort(block.getShort(positionArray[i], 0)).closeEntry();
                }
            }
        }
    }

    public static class Int96PositionsAppender
            implements PositionsAppender
    {
        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0));
                        blockBuilder.writeInt(block.getInt(position, SIZE_OF_LONG));
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    blockBuilder.writeLong(block.getLong(position, 0));
                    blockBuilder.writeInt(block.getInt(position, SIZE_OF_LONG));
                    blockBuilder.closeEntry();
                }
            }
        }
    }

    public static class Int128PositionsAppender
            implements PositionsAppender
    {
        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0));
                        blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    blockBuilder.writeLong(block.getLong(position, 0));
                    blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
                    blockBuilder.closeEntry();
                }
            }
        }
    }

    private static class CacheKey
    {
        private final Type type;
        private final Class<? extends Block> blockClass;

        private CacheKey(Type type, Class<? extends Block> blockClass)
        {
            this.type = requireNonNull(type, "type is null");
            this.blockClass = requireNonNull(blockClass, "blockClass is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return type.equals(cacheKey.type) && blockClass.equals(cacheKey.blockClass);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, blockClass);
        }
    }
}
