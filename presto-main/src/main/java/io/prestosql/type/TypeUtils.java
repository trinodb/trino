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
package io.prestosql.type;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.operator.HashGenerator;
import io.prestosql.operator.InterpretedHashGenerator;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.FixedWidthType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.StandardTypes.MAP;
import static io.prestosql.spi.type.StandardTypes.ROW;
import static java.lang.String.format;

public final class TypeUtils
{
    public static final int NULL_HASH_CODE = 0;

    private TypeUtils()
    {
    }

    public static int expectedValueSize(Type type, int defaultSize)
    {
        if (type instanceof FixedWidthType) {
            return ((FixedWidthType) type).getFixedSize();
        }
        // If bound on length of varchar or char is smaller than defaultSize, use that as expected size
        // The data can take up to 4 bytes per character due to UTF-8 encoding, but we assume it is ASCII and only needs one byte.
        if (type instanceof VarcharType) {
            return ((VarcharType) type).getLength()
                    .map(length -> Math.min(length, defaultSize))
                    .orElse(defaultSize);
        }
        if (type instanceof CharType) {
            return Math.min(((CharType) type).getLength(), defaultSize);
        }
        return defaultSize;
    }

    public static long hashPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        return type.hash(block, position);
    }

    public static long hashPosition(MethodHandle methodHandle, Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        try {
            if (type.getJavaType() == boolean.class) {
                return (long) methodHandle.invoke(type.getBoolean(block, position));
            }
            if (type.getJavaType() == long.class) {
                return (long) methodHandle.invoke(type.getLong(block, position));
            }
            if (type.getJavaType() == double.class) {
                return (long) methodHandle.invoke(type.getDouble(block, position));
            }
            if (type.getJavaType() == Slice.class) {
                return (long) methodHandle.invoke(type.getSlice(block, position));
            }
            if (!type.getJavaType().isPrimitive()) {
                return (long) methodHandle.invoke(type.getObject(block, position));
            }
            throw new UnsupportedOperationException("Unsupported native container type: " + type.getJavaType() + " with type " + type.getTypeSignature());
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public static boolean positionEqualsPosition(Type type, Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftIsNull = leftBlock.isNull(leftPosition);
        boolean rightIsNull = rightBlock.isNull(rightPosition);
        if (leftIsNull || rightIsNull) {
            return leftIsNull && rightIsNull;
        }
        return type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    public static long getHashPosition(List<? extends Type> hashTypes, Block[] hashBlocks, int position)
    {
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        Page page = new Page(hashBlocks);
        return hashGenerator.hashPosition(position, page);
    }

    public static Block getHashBlock(List<? extends Type> hashTypes, Block... hashBlocks)
    {
        checkArgument(hashTypes.size() == hashBlocks.length);
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        int positionCount = hashBlocks[0].getPositionCount();
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(positionCount);
        Page page = new Page(hashBlocks);
        for (int i = 0; i < positionCount; i++) {
            BIGINT.writeLong(builder, hashGenerator.hashPosition(i, page));
        }
        return builder.build();
    }

    public static Page getHashPage(Page page, List<? extends Type> types, List<Integer> hashChannels)
    {
        ImmutableList.Builder<Type> hashTypes = ImmutableList.builder();
        Block[] hashBlocks = new Block[hashChannels.size()];
        int hashBlockIndex = 0;

        for (int channel : hashChannels) {
            hashTypes.add(types.get(channel));
            hashBlocks[hashBlockIndex++] = page.getBlock(channel);
        }
        return page.appendColumn(getHashBlock(hashTypes.build(), hashBlocks));
    }

    public static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new PrestoException(NOT_SUPPORTED, errorMsg);
        }
    }

    public static String getDisplayLabel(Type type, boolean legacy)
    {
        if (legacy) {
            return getDisplayLabelForLegacyClients(type);
        }
        return type.getDisplayName();
    }

    private static String getDisplayLabelForLegacyClients(Type type)
    {
        if (type instanceof TimestampType && ((TimestampType) type).getPrecision() == TimestampType.DEFAULT_PRECISION) {
            return StandardTypes.TIMESTAMP;
        }
        if (type instanceof TimestampWithTimeZoneType && ((TimestampWithTimeZoneType) type).getPrecision() == TimestampWithTimeZoneType.DEFAULT_PRECISION) {
            return StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
        }
        if (type instanceof TimeType && ((TimeType) type).getPrecision() == TimeType.DEFAULT_PRECISION) {
            return StandardTypes.TIME;
        }
        if (type instanceof TimeWithTimeZoneType && ((TimeWithTimeZoneType) type).getPrecision() == TimeWithTimeZoneType.DEFAULT_PRECISION) {
            return StandardTypes.TIME_WITH_TIME_ZONE;
        }
        if (type instanceof ArrayType) {
            return ARRAY + "(" + getDisplayLabelForLegacyClients(((ArrayType) type).getElementType()) + ")";
        }
        if (type instanceof MapType) {
            return MAP + "(" + getDisplayLabelForLegacyClients(((MapType) type).getKeyType()) + ", " + getDisplayLabelForLegacyClients(((MapType) type).getValueType()) + ")";
        }
        if (type instanceof RowType) {
            return getRowDisplayLabelForLegacyClients((RowType) type);
        }

        return type.getDisplayName();
    }

    private static String getRowDisplayLabelForLegacyClients(RowType type)
    {
        List<String> fields = type.getFields().stream()
                .map(field -> {
                    String typeDisplayName = getDisplayLabelForLegacyClients(field.getType());
                    if (field.getName().isPresent()) {
                        return field.getName().get() + ' ' + typeDisplayName;
                    }
                    else {
                        return typeDisplayName;
                    }
                })
                .collect(toImmutableList());

        return format("%s(%s)", ROW, Joiner.on(", ").join(fields));
    }
}
