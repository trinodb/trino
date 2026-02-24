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
package io.trino.plugin.geospatial;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.trino.geospatial.serde.JtsGeometrySerde;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import org.locationtech.jts.geom.Geometry;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * Base class for geometry types (GeometryType and SphericalGeographyType).
 * Uses JTS Geometry as the stack type while storing EWKB bytes in blocks.
 */
public abstract class AbstractGeometryType
        extends AbstractVariableWidthType
{
    // Short strings are encoded with a negative length, so we have to encode the length in big-endian format
    private static final VarHandle INT_BE_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final int MAX_SHORT_FLAT_LENGTH = 3;

    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(GeometryTypeOperators.class, lookup(), Geometry.class);

    protected AbstractGeometryType(TypeSignature signature)
    {
        super(signature, Geometry.class);
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    // Escape hatch for direct EWKB access (used by connectors, optimized functions)
    @Override
    public Slice getSlice(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSlice(valuePosition);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(value, offset, length);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return JtsGeometrySerde.deserialize(getSlice(block, position));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        writeSlice(blockBuilder, JtsGeometrySerde.serialize((Geometry) value));
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        try {
            return JtsGeometrySerde.deserialize(getSlice(block, position)).toText();
        }
        catch (Exception e) {
            return "<invalid geometry>";
        }
    }

    // Helper methods for flat memory operations
    private static int readVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        int length = (int) INT_BE_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        if (length < 0) {
            int shortLength = fixedSizeSlice[fixedSizeOffset] & 0x7F;
            if (shortLength > MAX_SHORT_FLAT_LENGTH) {
                throw new IllegalArgumentException("Invalid short variable width length: " + shortLength);
            }
            return shortLength;
        }
        return length;
    }

    private static void writeFlatVariableLength(int length, byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        if (length < 0) {
            throw new IllegalArgumentException("Invalid variable width length: " + length);
        }
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            fixedSizeSlice[fixedSizeOffset] = (byte) (length | 0x80);
        }
        else {
            INT_BE_HANDLE.set(fixedSizeSlice, fixedSizeOffset, length);
        }
    }

    private static Slice readFlatToSlice(
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
    {
        int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
        byte[] bytes;
        int offset;
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            bytes = fixedSizeSlice;
            offset = fixedSizeOffset + 1;
        }
        else {
            bytes = variableSizeSlice;
            offset = variableSizeOffset;
        }
        return wrappedBuffer(bytes, offset, length);
    }

    private static void writeFlatFromSlice(
            Slice value,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
    {
        int length = value.length();
        writeFlatVariableLength(length, fixedSizeSlice, fixedSizeOffset);
        byte[] bytes;
        int offset;
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            bytes = fixedSizeSlice;
            offset = fixedSizeOffset + 1;
        }
        else {
            bytes = variableSizeSlice;
            offset = variableSizeOffset;
        }
        value.getBytes(0, bytes, offset, length);
    }

    /**
     * Operators for geometry types.
     * Strict binary equality is enforced to ensure consistency between Stack (Geometry)
     * and Block (Slice) representations. Topological equality must be checked via ST_Equals.
     */
    // This is a copy of AbstractVariableWidthType operators adapted for Geometry stack type. The
    // original implementation is inaccessible due to visibility restrictions.
    private static class GeometryTypeOperators
    {
        @ScalarOperator(READ_VALUE)
        private static Geometry readFlatToStack(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                @FlatVariableOffset int variableSizeOffset)
        {
            Slice slice = readFlatToSlice(fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset);
            return JtsGeometrySerde.deserialize(slice);
        }

        @ScalarOperator(READ_VALUE)
        private static void readFlatToBlock(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                @FlatVariableOffset int variableSizeOffset,
                BlockBuilder blockBuilder)
        {
            int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
            byte[] bytes;
            int offset;
            if (length <= MAX_SHORT_FLAT_LENGTH) {
                bytes = fixedSizeSlice;
                offset = fixedSizeOffset + 1;
            }
            else {
                bytes = variableSizeSlice;
                offset = variableSizeOffset;
            }
            ((VariableWidthBlockBuilder) blockBuilder).writeEntry(bytes, offset, length);
        }

        @ScalarOperator(READ_VALUE)
        private static void writeFlatFromStack(
                Geometry value,
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                @FlatVariableOffset int variableSizeOffset)
        {
            Slice slice = JtsGeometrySerde.serialize(value);
            writeFlatFromSlice(slice, fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset);
        }

        @ScalarOperator(READ_VALUE)
        private static void writeFlatFromBlock(
                @BlockPosition VariableWidthBlock block,
                @BlockIndex int position,
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                @FlatVariableOffset int variableSizeOffset)
        {
            Slice rawSlice = block.getRawSlice();
            int rawSliceOffset = block.getRawSliceOffset(position);
            int length = block.getSliceLength(position);

            writeFlatVariableLength(length, fixedSizeSlice, fixedSizeOffset);
            byte[] bytes;
            int offset;
            if (length <= MAX_SHORT_FLAT_LENGTH) {
                bytes = fixedSizeSlice;
                offset = fixedSizeOffset + 1;
            }
            else {
                bytes = variableSizeSlice;
                offset = variableSizeOffset;
            }
            rawSlice.getBytes(rawSliceOffset, bytes, offset, length);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(Geometry left, Geometry right)
        {
            Slice leftSlice = JtsGeometrySerde.serialize(left);
            Slice rightSlice = JtsGeometrySerde.serialize(right);
            return leftSlice.equals(rightSlice);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @BlockPosition VariableWidthBlock leftBlock,
                @BlockIndex int leftPosition,
                @BlockPosition VariableWidthBlock rightBlock,
                @BlockIndex int rightPosition)
        {
            Slice leftRawSlice = leftBlock.getRawSlice();
            int leftRawSliceOffset = leftBlock.getRawSliceOffset(leftPosition);
            int leftLength = leftBlock.getSliceLength(leftPosition);

            Slice rightRawSlice = rightBlock.getRawSlice();
            int rightRawSliceOffset = rightBlock.getRawSliceOffset(rightPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);

            return leftRawSlice.equals(leftRawSliceOffset, leftLength, rightRawSlice, rightRawSliceOffset, rightLength);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                Geometry left,
                @BlockPosition VariableWidthBlock rightBlock,
                @BlockIndex int rightPosition)
        {
            Slice leftSlice = JtsGeometrySerde.serialize(left);
            Slice rightRawSlice = rightBlock.getRawSlice();
            int rightOffset = rightBlock.getRawSliceOffset(rightPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);
            return leftSlice.equals(0, leftSlice.length(), rightRawSlice, rightOffset, rightLength);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @BlockPosition VariableWidthBlock leftBlock,
                @BlockIndex int leftPosition,
                Geometry right)
        {
            return equalOperator(right, leftBlock, leftPosition);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @FlatFixed byte[] leftFixedSizeSlice,
                @FlatFixedOffset int leftFixedSizeOffset,
                @FlatVariableWidth byte[] leftVariableSizeSlice,
                @FlatVariableOffset int leftVariableSizeOffset,
                @FlatFixed byte[] rightFixedSizeSlice,
                @FlatFixedOffset int rightFixedSizeOffset,
                @FlatVariableWidth byte[] rightVariableSizeSlice,
                @FlatVariableOffset int rightVariableSizeOffset)
        {
            int leftLength = readVariableWidthLength(leftFixedSizeSlice, leftFixedSizeOffset);
            int rightLength = readVariableWidthLength(rightFixedSizeSlice, rightFixedSizeOffset);
            if (leftLength != rightLength) {
                return false;
            }
            if (leftLength <= MAX_SHORT_FLAT_LENGTH) {
                return ((int) INT_BE_HANDLE.get(leftFixedSizeSlice, leftFixedSizeOffset)) ==
                        ((int) INT_BE_HANDLE.get(rightFixedSizeSlice, rightFixedSizeOffset));
            }
            return Arrays.equals(
                    leftVariableSizeSlice,
                    leftVariableSizeOffset,
                    leftVariableSizeOffset + leftLength,
                    rightVariableSizeSlice,
                    rightVariableSizeOffset,
                    rightVariableSizeOffset + rightLength);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @BlockPosition VariableWidthBlock leftBlock,
                @BlockIndex int leftPosition,
                @FlatFixed byte[] rightFixedSizeSlice,
                @FlatFixedOffset int rightFixedSizeOffset,
                @FlatVariableWidth byte[] rightVariableSizeSlice,
                @FlatVariableOffset int rightVariableSizeOffset)
        {
            return equalOperator(
                    rightFixedSizeSlice,
                    rightFixedSizeOffset,
                    rightVariableSizeSlice,
                    rightVariableSizeOffset,
                    leftBlock,
                    leftPosition);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @FlatFixed byte[] leftFixedSizeSlice,
                @FlatFixedOffset int leftFixedSizeOffset,
                @FlatVariableWidth byte[] leftVariableSizeSlice,
                @FlatVariableOffset int leftVariableSizeOffset,
                @BlockPosition VariableWidthBlock rightBlock,
                @BlockIndex int rightPosition)
        {
            int leftLength = readVariableWidthLength(leftFixedSizeSlice, leftFixedSizeOffset);

            Slice rightRawSlice = rightBlock.getRawSlice();
            int rightRawSliceOffset = rightBlock.getRawSliceOffset(rightPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);

            if (leftLength != rightLength) {
                return false;
            }

            byte[] leftBytes;
            int leftOffset;
            if (leftLength <= MAX_SHORT_FLAT_LENGTH) {
                leftBytes = leftFixedSizeSlice;
                leftOffset = leftFixedSizeOffset + 1;
            }
            else {
                leftBytes = leftVariableSizeSlice;
                leftOffset = leftVariableSizeOffset;
            }
            return rightRawSlice.equals(rightRawSliceOffset, rightLength, wrappedBuffer(leftBytes, leftOffset, leftLength), 0, leftLength);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                Geometry left,
                @FlatFixed byte[] rightFixedSizeSlice,
                @FlatFixedOffset int rightFixedSizeOffset,
                @FlatVariableWidth byte[] rightVariableSizeSlice,
                @FlatVariableOffset int rightVariableSizeOffset)
        {
            Slice leftSlice = JtsGeometrySerde.serialize(left);
            Slice rightSlice = readFlatToSlice(rightFixedSizeSlice, rightFixedSizeOffset, rightVariableSizeSlice, rightVariableSizeOffset);
            return leftSlice.equals(rightSlice);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @FlatFixed byte[] leftFixedSizeSlice,
                @FlatFixedOffset int leftFixedSizeOffset,
                @FlatVariableWidth byte[] leftVariableSizeSlice,
                @FlatVariableOffset int leftVariableSizeOffset,
                Geometry right)
        {
            return equalOperator(right, leftFixedSizeSlice, leftFixedSizeOffset, leftVariableSizeSlice, leftVariableSizeOffset);
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(Geometry value)
        {
            return XxHash64.hash(JtsGeometrySerde.serialize(value));
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(@BlockPosition VariableWidthBlock block, @BlockIndex int position)
        {
            return XxHash64.hash(block.getRawSlice(), block.getRawSliceOffset(position), block.getSliceLength(position));
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                @FlatVariableOffset int variableSizeOffset)
        {
            int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
            byte[] bytes;
            int offset;
            if (length <= MAX_SHORT_FLAT_LENGTH) {
                bytes = fixedSizeSlice;
                offset = fixedSizeOffset + 1;
            }
            else {
                bytes = variableSizeSlice;
                offset = variableSizeOffset;
            }
            return XxHash64.hash(wrappedBuffer(bytes, offset, length));
        }
    }
}
