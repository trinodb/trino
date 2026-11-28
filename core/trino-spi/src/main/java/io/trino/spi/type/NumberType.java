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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.trino.spi.Unstable;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.block.VariableWidthValues;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlNullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TrinoNumber.HEADER_LENGTH;
import static io.trino.spi.type.TrinoNumber.MARKER_SCALE_INFINITY;
import static io.trino.spi.type.TrinoNumber.MARKER_SCALE_NAN;
import static io.trino.spi.type.TrinoNumber.header;
import static io.trino.spi.type.TrinoNumber.negatedFromHeader;
import static io.trino.spi.type.TrinoNumber.scaleFromHeader;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static io.trino.spi.type.Verify.verify;
import static java.lang.Math.abs;
import static java.lang.Math.log10;
import static java.lang.Math.multiplyHigh;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * A high precision decimal type. The precise limits and rounding behavior are considered an
 * implementation detail and may evolve over time together with the unstable binary format.
 * <p>
 * Stack representation of this type is {@link TrinoNumber} which wraps a slice with the format documented below.
 * <b>Note:</b> the binary format is not stable and may change between releases. Only the Java API is considered stable.
 * <h2>Current unstable binary format</h2>
 * <pre>
 *    ┌───────────────────────┬─────────────────────────┐
 *    │  header (16-bit, LE)  │ unsigned magnitude (BE) │
 *    └───────────────────────┴─────────────────────────┘
 *     └─────────────────────┘
 *             2 bytes                0+ bytes
 * </pre>
 * <dl>
 *     <dt>header</dt>
 *     <dd>A 16 bit number stored in little-endian byte order.
 *         The most significant bit indicates whether the number is negative (1) or non-negative (0).
 *         The remaining 15 bits store the scale + {@value TrinoNumber#SCALE_BASE} as an unsigned integer.
 *      </dd>
 *     <dt>unsigned magnitude</dt>
 *     <dd>A big-endian minimal representation of the absolute value of the unscaled integer containing digits of the value.
 *         The representation is minimal, i.e. it does not have leading zero bytes.
 *         This guarantees that two numerically equal values have the same binary representation.
 *     </dd>
 * </dl>
 * <p>
 * A value stored like this represents the following number:
 * <pre>
 *     (-1)^sign * magnitude * 10^(-scale)
 * </pre>
 */
public class NumberType
        extends AbstractVariableWidthType
{
    public static final String NAME = "number";
    public static final NumberType NUMBER = new NumberType();

    // Precision loss prevents creation of values that would occupy very large amount of memory.
    // Max precision is implementation detail and subject to change.
    static final int MAX_DECIMAL_PRECISION = 200;

    private static final VarHandle SHORT_LE_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final short NAN_HEADER = header(false, MARKER_SCALE_NAN);
    private static final double LOG10_2 = log10(2);
    // tolerance for floating point error of a log10 estimate: must be greater than the rounding error ~1e-11
    // and lower than the smallest difference between non-equal estimates ~4e-4
    private static final double LOG10_MARGIN = 1e-9;
    // 10^18 is the largest power of ten in a long
    private static final long[] POWERS_OF_TEN = LongStream.iterate(1, power -> power * 10).limit(19).toArray();

    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(NumberType.class, lookup(), TrinoNumber.class);

    private NumberType()
    {
        super(new TypeDescriptor(NAME), TrinoNumber.class);
    }

    @Override
    public String getDisplayName()
    {
        return NAME;
    }

    @Unstable
    public static int currentMaxPrecision()
    {
        return MAX_DECIMAL_PRECISION;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlNumber(((TrinoNumber) getObject(block, position)).toBigDecimal());
    }

    @Override
    public Object getObject(Block block, int position)
    {
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        Slice slice = VariableWidthValues.getSlice(valueBlock, valuePosition);
        return new TrinoNumber(slice);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        TrinoNumber number = (TrinoNumber) value;
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(number.bytes());
    }

    @Override
    public boolean equals(Object other)
    {
        return other == NUMBER;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @ScalarOperator(EQUAL)
    static boolean equalOperator(TrinoNumber left, TrinoNumber right)
    {
        // NaN is not equal to any value, including itself
        return left.bytes().equals(right.bytes()) && !left.isNaN();
    }

    @ScalarOperator(IDENTICAL)
    static boolean identicalOperator(@SqlNullable TrinoNumber left, @SqlNullable TrinoNumber right)
    {
        if (left == null || right == null) {
            return left == right;
        }
        return left.bytes().equals(right.bytes());
    }

    @Override
    public int getFlatFixedSize()
    {
        // Based on MAX_DECIMAL_PRECISION value
        return Byte.BYTES;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        ValueBlock variableWidthBlock = block.getUnderlyingValueBlock();
        int length = VariableWidthValues.getSliceLength(variableWidthBlock, block.getUnderlyingValuePosition(position));
        verify(length <= 0xFF, "Length out of range: %s", length);
        return length;
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        return readFlatLength(fixedSizeSlice, fixedSizeOffset);
    }

    @ScalarOperator(READ_VALUE)
    private static TrinoNumber readFlatToStack(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        int length = readFlatLength(fixedSizeSlice, fixedSizeOffset);
        return new TrinoNumber(wrappedBuffer(variableSizeSlice, variableSizeOffset, length));
    }

    @ScalarOperator(READ_VALUE)
    private static void readFlatToBlock(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset,
            BlockBuilder blockBuilder)
    {
        int length = readFlatLength(fixedSizeSlice, fixedSizeOffset);
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(variableSizeSlice, variableSizeOffset, length);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlatFromStack(
            TrinoNumber value,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        Slice bytes = value.bytes();
        int length = bytes.length();
        verify(length <= 0xFF, "Length out of range: %s", length);
        fixedSizeSlice[fixedSizeOffset] = (byte) length;
        bytes.getBytes(0, variableSizeSlice, variableSizeOffset, length);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlatFromBlock(
            @BlockPosition ValueBlock block,
            @BlockIndex int position,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        int length = VariableWidthValues.getSliceLength(block, position);

        verify(length <= 0xFF, "Length out of range: %s", length);
        fixedSizeSlice[fixedSizeOffset] = (byte) length;
        VariableWidthValues.copyValue(block, position, variableSizeSlice, variableSizeOffset, length);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition ValueBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition ValueBlock rightBlock, @BlockIndex int rightPosition)
    {
        return identicalOperator(leftBlock, leftPosition, rightBlock, rightPosition) && !isNaN(leftBlock, leftPosition);
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
        return identicalOperator(leftFixedSizeSlice, leftFixedSizeOffset, leftVariableSizeSlice, leftVariableSizeOffset, rightFixedSizeSlice, rightFixedSizeOffset, rightVariableSizeSlice, rightVariableSizeOffset) &&
                !isNaN(leftFixedSizeSlice, leftFixedSizeOffset, leftVariableSizeSlice, leftVariableSizeOffset);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] leftVariableSizeSlice,
            @FlatVariableOffset int leftVariableSizeOffset,
            @BlockPosition ValueBlock rightBlock,
            @BlockIndex int rightPosition)
    {
        return identicalOperator(leftFixedSizeSlice, leftFixedSizeOffset, leftVariableSizeSlice, leftVariableSizeOffset, rightBlock, rightPosition) &&
                !isNaN(leftFixedSizeSlice, leftFixedSizeOffset, leftVariableSizeSlice, leftVariableSizeOffset);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(
            @BlockPosition ValueBlock leftBlock,
            @BlockIndex int leftPosition,
            @FlatFixed byte[] rightFixedSizeSlice,
            @FlatFixedOffset int rightFixedSizeOffset,
            @FlatVariableWidth byte[] rightVariableSizeSlice,
            @FlatVariableOffset int rightVariableSizeOffset)
    {
        return equalOperator(rightFixedSizeSlice, rightFixedSizeOffset, rightVariableSizeSlice, rightVariableSizeOffset, leftBlock, leftPosition);
    }

    @ScalarOperator(IDENTICAL)
    private static boolean identicalOperator(@BlockPosition ValueBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition ValueBlock rightBlock, @BlockIndex int rightPosition)
    {
        return VariableWidthValues.valueEquals(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @ScalarOperator(IDENTICAL)
    private static boolean identicalOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] leftVariableSizeSlice,
            @FlatVariableOffset int leftVariableSizeOffset,
            @FlatFixed byte[] rightFixedSizeSlice,
            @FlatFixedOffset int rightFixedSizeOffset,
            @FlatVariableWidth byte[] rightVariableSizeSlice,
            @FlatVariableOffset int rightVariableSizeOffset)
    {
        int leftLength = readFlatLength(leftFixedSizeSlice, leftFixedSizeOffset);
        int rightLength = readFlatLength(rightFixedSizeSlice, rightFixedSizeOffset);
        return Arrays.equals(
                leftVariableSizeSlice,
                leftVariableSizeOffset,
                leftVariableSizeOffset + leftLength,
                rightVariableSizeSlice,
                rightVariableSizeOffset,
                rightVariableSizeOffset + rightLength);
    }

    @ScalarOperator(IDENTICAL)
    private static boolean identicalOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] leftVariableSizeSlice,
            @FlatVariableOffset int leftVariableSizeOffset,
            @BlockPosition ValueBlock rightBlock,
            @BlockIndex int rightPosition)
    {
        return VariableWidthValues.valueEquals(
                rightBlock,
                rightPosition,
                leftVariableSizeSlice,
                leftVariableSizeOffset,
                readFlatLength(leftFixedSizeSlice, leftFixedSizeOffset));
    }

    @ScalarOperator(IDENTICAL)
    private static boolean identicalOperator(
            @BlockPosition ValueBlock leftBlock,
            @BlockIndex int leftPosition,
            @FlatFixed byte[] rightFixedSizeSlice,
            @FlatFixedOffset int rightFixedSizeOffset,
            @FlatVariableWidth byte[] rightVariableSizeSlice,
            @FlatVariableOffset int rightVariableSizeOffset)
    {
        return identicalOperator(rightFixedSizeSlice, rightFixedSizeOffset, rightVariableSizeSlice, rightVariableSizeOffset, leftBlock, leftPosition);
    }

    private static boolean isNaN(ValueBlock block, int position)
    {
        Slice rawSlice = VariableWidthValues.getRawSlice(block);
        return isNaN(rawSlice.byteArray(), rawSlice.byteArrayOffset() + VariableWidthValues.getRawSliceOffset(block, position), VariableWidthValues.getSliceLength(block, position));
    }

    private static boolean isNaN(byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        return isNaN(variableSizeSlice, variableSizeOffset, readFlatLength(fixedSizeSlice, fixedSizeOffset));
    }

    private static boolean isNaN(byte[] bytes, int offset, int length)
    {
        return length == HEADER_LENGTH && (short) SHORT_LE_HANDLE.get(bytes, offset) == NAN_HEADER;
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(TrinoNumber value)
    {
        Slice slice = value.bytes();
        return XxHash64.hash(slice);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition ValueBlock block, @BlockIndex int position)
    {
        return VariableWidthValues.valueHash(block, position);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        int length = readFlatLength(fixedSizeSlice, fixedSizeOffset);
        return XxHash64.hash(variableSizeSlice, variableSizeOffset, length);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(TrinoNumber left, TrinoNumber right)
    {
        return compareNaNLast(left, right);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(@BlockPosition ValueBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition ValueBlock rightBlock, @BlockIndex int rightPosition)
    {
        return compareNaNLast(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] leftVariableSizeSlice,
            @FlatVariableOffset int leftVariableSizeOffset,
            @FlatFixed byte[] rightFixedSizeSlice,
            @FlatFixedOffset int rightFixedSizeOffset,
            @FlatVariableWidth byte[] rightVariableSizeSlice,
            @FlatVariableOffset int rightVariableSizeOffset)
    {
        return compareNaNLast(
                leftVariableSizeSlice,
                leftVariableSizeOffset,
                readFlatLength(leftFixedSizeSlice, leftFixedSizeOffset),
                rightVariableSizeSlice,
                rightVariableSizeOffset,
                readFlatLength(rightFixedSizeSlice, rightFixedSizeOffset));
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] leftVariableSizeSlice,
            @FlatVariableOffset int leftVariableSizeOffset,
            @BlockPosition ValueBlock rightBlock,
            @BlockIndex int rightPosition)
    {
        Slice rightRawSlice = VariableWidthValues.getRawSlice(rightBlock);
        return compareNaNLast(
                leftVariableSizeSlice,
                leftVariableSizeOffset,
                readFlatLength(leftFixedSizeSlice, leftFixedSizeOffset),
                rightRawSlice.byteArray(),
                rightRawSlice.byteArrayOffset() + VariableWidthValues.getRawSliceOffset(rightBlock, rightPosition),
                VariableWidthValues.getSliceLength(rightBlock, rightPosition));
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(
            @BlockPosition ValueBlock leftBlock,
            @BlockIndex int leftPosition,
            @FlatFixed byte[] rightFixedSizeSlice,
            @FlatFixedOffset int rightFixedSizeOffset,
            @FlatVariableWidth byte[] rightVariableSizeSlice,
            @FlatVariableOffset int rightVariableSizeOffset)
    {
        return -comparisonOperator(rightFixedSizeSlice, rightFixedSizeOffset, rightVariableSizeSlice, rightVariableSizeOffset, leftBlock, leftPosition);
    }

    private static int compareNaNLast(TrinoNumber left, TrinoNumber right)
    {
        Slice leftBytes = left.bytes();
        Slice rightBytes = right.bytes();
        return compareNaNLast(
                leftBytes.byteArray(),
                leftBytes.byteArrayOffset(),
                leftBytes.length(),
                rightBytes.byteArray(),
                rightBytes.byteArrayOffset(),
                rightBytes.length());
    }

    private static int compareNaNLast(ValueBlock leftBlock, int leftPosition, ValueBlock rightBlock, int rightPosition)
    {
        Slice leftRawSlice = VariableWidthValues.getRawSlice(leftBlock);
        Slice rightRawSlice = VariableWidthValues.getRawSlice(rightBlock);
        return compareNaNLast(
                leftRawSlice.byteArray(),
                leftRawSlice.byteArrayOffset() + VariableWidthValues.getRawSliceOffset(leftBlock, leftPosition),
                VariableWidthValues.getSliceLength(leftBlock, leftPosition),
                rightRawSlice.byteArray(),
                rightRawSlice.byteArrayOffset() + VariableWidthValues.getRawSliceOffset(rightBlock, rightPosition),
                VariableWidthValues.getSliceLength(rightBlock, rightPosition));
    }

    /// Compares encoded values in [TrinoNumber.AsBigDecimal#COMPARE_NAN_LAST] order.
    private static int compareNaNLast(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        Rank leftRank = rank(left, leftOffset, leftLength);
        Rank rightRank = rank(right, rightOffset, rightLength);
        if (leftRank != rightRank) {
            return leftRank.compareTo(rightRank);
        }
        return switch (leftRank) {
            case NEGATIVE -> compareMagnitudes(right, rightOffset, rightLength, left, leftOffset, leftLength);
            case POSITIVE -> compareMagnitudes(left, leftOffset, leftLength, right, rightOffset, rightLength);
            case NEGATIVE_INFINITY, ZERO, POSITIVE_INFINITY, NAN -> 0;
        };
    }

    private static Rank rank(byte[] bytes, int offset, int length)
    {
        short header = (short) SHORT_LE_HANDLE.get(bytes, offset);
        boolean negated = negatedFromHeader(header);
        if (length == HEADER_LENGTH) {
            return switch (scaleFromHeader(header)) {
                case 0 -> Rank.ZERO;
                case MARKER_SCALE_NAN -> Rank.NAN;
                case MARKER_SCALE_INFINITY -> negated ? Rank.NEGATIVE_INFINITY : Rank.POSITIVE_INFINITY;
                default -> throw new IllegalArgumentException("Invalid header for TrinoNumber with unscaled 0 value: " + header);
            };
        }
        return negated ? Rank.NEGATIVE : Rank.POSITIVE;
    }

    /// Kinds of NUMBER values, declared in [TrinoNumber.AsBigDecimal#COMPARE_NAN_LAST] order.
    private enum Rank
    {
        NEGATIVE_INFINITY,
        NEGATIVE,
        ZERO,
        POSITIVE,
        POSITIVE_INFINITY,
        NAN,
    }

    /// Compares absolute values of two finite non-zero encoded values.
    private static int compareMagnitudes(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        int leftScale = scaleFromHeader((short) SHORT_LE_HANDLE.get(left, leftOffset));
        int rightScale = scaleFromHeader((short) SHORT_LE_HANDLE.get(right, rightOffset));
        int leftMagnitudeOffset = leftOffset + HEADER_LENGTH;
        int leftMagnitudeLength = leftLength - HEADER_LENGTH;
        int rightMagnitudeOffset = rightOffset + HEADER_LENGTH;
        int rightMagnitudeLength = rightLength - HEADER_LENGTH;

        if (leftScale == rightScale) {
            // magnitudes have no leading zero bytes, so a longer magnitude is larger
            if (leftMagnitudeLength != rightMagnitudeLength) {
                return Integer.compare(leftMagnitudeLength, rightMagnitudeLength);
            }
            return Arrays.compareUnsigned(
                    left,
                    leftMagnitudeOffset,
                    leftMagnitudeOffset + leftMagnitudeLength,
                    right,
                    rightMagnitudeOffset,
                    rightMagnitudeOffset + rightMagnitudeLength);
        }

        if (leftMagnitudeLength < Long.BYTES && rightMagnitudeLength < Long.BYTES && abs(leftScale - rightScale) < POWERS_OF_TEN.length) {
            long leftMagnitude = readMagnitude(left, leftMagnitudeOffset, leftMagnitudeLength);
            long rightMagnitude = readMagnitude(right, rightMagnitudeOffset, rightMagnitudeLength);
            // value = magnitude * 10^-scale; rescale the one with the smaller scale
            if (leftScale < rightScale) {
                return compareScaled(leftMagnitude, POWERS_OF_TEN[rightScale - leftScale], rightMagnitude);
            }
            return -compareScaled(rightMagnitude, POWERS_OF_TEN[leftScale - rightScale], leftMagnitude);
        }

        return compareMagnitudesSlow(left, leftMagnitudeOffset, leftMagnitudeLength, leftScale, right, rightMagnitudeOffset, rightMagnitudeLength, rightScale);
    }

    private static int compareMagnitudesSlow(byte[] left, int leftMagnitudeOffset, int leftMagnitudeLength, int leftScale, byte[] right, int rightMagnitudeOffset, int rightMagnitudeLength, int rightScale)
    {
        // magnitude is in [2^(bits - 1), 2^bits), so log10(value) is in [(bits - 1) * log10(2) - scale, bits * log10(2) - scale)
        double leftLog10Low = (bitLength(left, leftMagnitudeOffset, leftMagnitudeLength) - 1) * LOG10_2 - leftScale;
        double rightLog10Low = (bitLength(right, rightMagnitudeOffset, rightMagnitudeLength) - 1) * LOG10_2 - rightScale;
        // the margin covers the width of the interval plus floating point error
        if (leftLog10Low > rightLog10Low + LOG10_2 + LOG10_MARGIN) {
            return 1;
        }
        if (rightLog10Low > leftLog10Low + LOG10_2 + LOG10_MARGIN) {
            return -1;
        }
        return new BigDecimal(new BigInteger(1, left, leftMagnitudeOffset, leftMagnitudeLength), leftScale)
                .compareTo(new BigDecimal(new BigInteger(1, right, rightMagnitudeOffset, rightMagnitudeLength), rightScale));
    }

    private static int compareScaled(long magnitude, long multiplier, long other)
    {
        if (multiplyHigh(magnitude, multiplier) != 0) {
            // magnitude * multiplier exceeds 64 bits, while other fits in 56 bits
            return 1;
        }
        return Long.compareUnsigned(magnitude * multiplier, other);
    }

    private static long readMagnitude(byte[] bytes, int offset, int length)
    {
        long magnitude = 0;
        for (int i = 0; i < length; i++) {
            magnitude = (magnitude << 8) | (bytes[offset + i] & 0xFF);
        }
        return magnitude;
    }

    private static int bitLength(byte[] bytes, int offset, int length)
    {
        // first byte is non-zero as magnitude has no leading zero bytes
        return (length - 1) * Byte.SIZE + (Integer.SIZE - Integer.numberOfLeadingZeros(bytes[offset] & 0xFF));
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(TrinoNumber left, TrinoNumber right)
    {
        return !left.isNaN() && !right.isNaN() && compareNaNLast(left, right) < 0;
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(@BlockPosition ValueBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition ValueBlock rightBlock, @BlockIndex int rightPosition)
    {
        return !isNaN(leftBlock, leftPosition) && !isNaN(rightBlock, rightPosition) && compareNaNLast(leftBlock, leftPosition, rightBlock, rightPosition) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(TrinoNumber left, TrinoNumber right)
    {
        return !left.isNaN() && !right.isNaN() && compareNaNLast(left, right) <= 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(@BlockPosition ValueBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition ValueBlock rightBlock, @BlockIndex int rightPosition)
    {
        return !isNaN(leftBlock, leftPosition) && !isNaN(rightBlock, rightPosition) && compareNaNLast(leftBlock, leftPosition, rightBlock, rightPosition) <= 0;
    }

    private static int readFlatLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        return fixedSizeSlice[fixedSizeOffset] & 0xFF;
    }
}
