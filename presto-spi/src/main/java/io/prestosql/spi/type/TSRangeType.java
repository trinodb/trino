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
package io.prestosql.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.VariableWidthBlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.prestosql.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.function.OperatorType.COMPARISON;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Math.abs;
import static java.lang.Math.signum;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * TSRangeType is an interval between two timestamps and additionally encodes whether first/last timestamp is inclusive or exclusive.
 */
public class TSRangeType
        extends AbstractType
        implements FixedWidthType
{
    public static final String TSRANGE_SIGNATURE = "tsrange";
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(TSRangeType.class, lookup(), Slice.class);

    public static final int MAX_PRECISION = 9;
    public static final int DEFAULT_PRECISION = TimestampType.DEFAULT_PRECISION;

    private static final TSRangeType[] TYPES = new TSRangeType[MAX_PRECISION + 1];

    static {
        for (int precision = 0; precision <= MAX_PRECISION; precision++) {
            TYPES[precision] = new TSRangeType(precision);
        }
    }

    public static final TSRangeType TSRANGE_SECONDS = createTSRangeType(0);
    public static final TSRangeType TSRANGE_MILLIS = createTSRangeType(3);
    public static final TSRangeType TSRANGE_MICROS = createTSRangeType(6);
    public static final TSRangeType TSRANGE_NANOS = createTSRangeType(9);

    private final int precision;

    private TSRangeType(int precision)
    {
        super(new TypeSignature(TSRANGE_SIGNATURE, TypeSignatureParameter.numericParameter(precision)), Slice.class);
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format("Precision must be in the range [0, %s]", MAX_PRECISION));
        }
        this.precision = precision;
    }

    public static TSRangeType createTSRangeType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE,
                    format("tsrange precision must be in range [0, %s] but got: %s", MAX_PRECISION, precision));
        }
        return TYPES[precision];
    }

    public int getPrecision()
    {
        return precision;
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

    // internal structure of the tsrangeType - encoded as a slice of 2 longs

    public static final int RANGE_LENGTH = 2 * SIZE_OF_LONG;
    public static final int RANGE_ENTRIES = 2;
    public static final int LOWER_OFFSET = 0;
    public static final int UPPER_OFFSET = SIZE_OF_LONG;

    static final long EMPTY_VALUE = Long.MAX_VALUE - 1;

    public static final Slice EMPTY_RANGE_SLICE;

    static {
        Slice slice = Slices.allocate(RANGE_LENGTH);
        slice.setLong(0, -abs(TSRange.EMPTY_RANGE.getLower()));
        slice.setLong(SIZE_OF_LONG, -abs(TSRange.EMPTY_RANGE.getUpper()));

        EMPTY_RANGE_SLICE = slice;

        if (!TSRange.EMPTY_RANGE.isEmpty() || TSRange.EMPTY_RANGE.getLower() != EMPTY_VALUE) {
            throw new IllegalStateException("inconsistent slice <> tsrange state");
        }
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public int getFixedSize()
    {
        return RANGE_LENGTH;
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new VariableWidthBlockBuilder(null, RANGE_ENTRIES, RANGE_LENGTH);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return createFixedSizeBlockBuilder(0);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createFixedSizeBlockBuilder(0);
    }

    @Override
    public void writeObject(BlockBuilder out, Object value)
    {
        if (!(value instanceof TSRange)) {
            throw new IllegalArgumentException("value must be an instance of tsrange");
        }
        final TSRange range = (TSRange) value;
        out.writeLong(range.isLowerClosed() ? range.getLower() : -range.getLower());
        out.writeLong(range.isUpperClosed() ? range.getUpper() : -range.getUpper());
        out.closeEntry();
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        final long lower = block.getLong(position, LOWER_OFFSET);
        final long upper = block.getLong(position, UPPER_OFFSET);
        return TSRange.createTSRange(abs(lower), abs(upper), lower >= 0, upper >= 0);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position, LOWER_OFFSET));
            blockBuilder.writeLong(block.getLong(position, UPPER_OFFSET));
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        if (length != RANGE_LENGTH) {
            throw new IllegalStateException("Expected entry size to be exactly " + RANGE_LENGTH + " but was " + length);
        }
        blockBuilder.writeLong(value.getLong(offset));
        blockBuilder.writeLong(value.getLong(offset + UPPER_OFFSET));
        blockBuilder.closeEntry();
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return createTSRangeSlice(block.getLong(position, LOWER_OFFSET), block.getLong(position, UPPER_OFFSET));
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    private static long hashCodeOperator(Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Block block, @BlockIndex int position)
    {
        return XxHash64.hash(createTSRangeSlice(block.getLong(position, LOWER_OFFSET), block.getLong(position, UPPER_OFFSET)));
    }

    @ScalarOperator(COMPARISON)
    private static long comparisonOperator(Slice slice, Slice other)
    {
        return TSRangeType.compare(slice, other);
    }

    @ScalarOperator(COMPARISON)
    private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition,
            @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        final long leftLower = leftBlock.getLong(leftPosition, LOWER_OFFSET);
        final long leftUpper = leftBlock.getLong(leftPosition, UPPER_OFFSET);
        final long rightLower = rightBlock.getLong(rightPosition, LOWER_OFFSET);
        final long rightUpper = rightBlock.getLong(rightPosition, UPPER_OFFSET);
        return comparisonOperator(createTSRangeSlice(leftLower, leftUpper), createTSRangeSlice(rightLower, rightUpper));
    }

    @ScalarOperator(EQUAL)
    public static boolean equalOperator(Slice left, Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(EQUAL)
    public static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition,
            @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return leftBlock.getLong(leftPosition, LOWER_OFFSET) == rightBlock.getLong(rightPosition, LOWER_OFFSET) &&
                leftBlock.getLong(leftPosition, UPPER_OFFSET) == rightBlock.getLong(rightPosition, UPPER_OFFSET);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOperator(Slice left, Slice right)
    {
        return TSRangeType.compare(left, right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqualOperator(Slice left, Slice right)
    {
        return TSRangeType.compare(left, right) <= 0;
    }

    public static Slice empty()
    {
        return EMPTY_RANGE_SLICE;
    }

    public static Slice createTSRangeSlice(long lower, long right)
    {
        return createTSRangeSlice(abs(lower), abs(right), lower >= 0, right >= 0);
    }

    public static Slice createTSRangeSlice(long lower, long upper, boolean lowerClosed, boolean upperClosed)
    {
        if (lower > upper) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "invalid values for slice creation:, " + lower + "," + upper + " bounds=" + lowerClosed + "," + upperClosed);
        }

        if (lower < 0 || upper < 0) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "invalid values, negative timestamps: " + lower + "," + upper + " bounds=" + lowerClosed + "," + upperClosed);
        }

        if (lower == upper && !(lowerClosed && upperClosed)) {
            return empty();
        }

        final Slice slice = Slices.allocate(RANGE_LENGTH);
        slice.setLong(0, lowerClosed ? lower : -lower);
        slice.setLong(SIZE_OF_LONG, upperClosed ? upper : -upper);
        return slice;
    }

    public static Slice tsrangeLongToSlice(TSRange tsrange)
    {
        if (tsrange.isEmpty()) {
            return EMPTY_RANGE_SLICE;
        }

        return createTSRangeSlice(tsrange.getLower(), tsrange.getUpper(), tsrange.isLowerClosed(), tsrange.isUpperClosed());
    }

    public static TSRange sliceToTSRange(Slice slice)
    {
        if (EMPTY_RANGE_SLICE.equals(slice)) {
            return TSRange.EMPTY_RANGE;
        }

        final long lowerValue = getLower(slice);
        final long upperValue = getUpper(slice);

        return TSRange.createTSRange(abs(lowerValue), abs(upperValue), lowerValue >= 0, upperValue >= 0);
    }

    public static long getLower(Slice slice)
    {
        return slice.getLong(0);
    }

    public static long getUpper(Slice slice)
    {
        return slice.getLong(SIZE_OF_LONG);
    }

    public static long getFirstInclusive(Slice slice)
    {
        final long lower = getLower(slice);
        if (lower >= 0) {
            return lower;
        }
        else {
            return -lower + 1;
        }
    }

    public static long getLastInclusive(Slice slice)
    {
        final long right = getUpper(slice);
        if (right >= 0) {
            return right;
        }
        else {
            return -right - 1;
        }
    }

    public static int compare(Slice slice, Slice other)
    {
        final int lowerBoundsCompared = compareLower(slice, other);
        if (lowerBoundsCompared != 0) {
            return lowerBoundsCompared;
        }
        else {
            return compareUpper(slice, other);
        }
    }

    public static int compareLower(Slice slice, Slice other)
    {
        return compareSliceBounds(slice, other, LOWER_OFFSET);
    }

    public static int compareUpper(Slice slice, Slice other)
    {
        return compareSliceBounds(slice, other, UPPER_OFFSET);
    }

    private static int compareSliceBounds(Slice leftSlice, Slice rightSlice, int offsetValue)
    {
        final long leftValue = leftSlice.getLong(offsetValue);
        final long rightValue = rightSlice.getLong(offsetValue);
        final int compareAbsoluteValues = Long.compare(abs(leftValue), abs(rightValue));
        if (compareAbsoluteValues != 0) {
            return compareAbsoluteValues;
        }
        final int boundTypeCompare = Boolean.compare(leftValue >= 0, rightValue >= 0);
        if (offsetValue == LOWER_OFFSET) {
            return -boundTypeCompare;
        }
        else {
            return boundTypeCompare;
        }
    }

    public static boolean adjacent(Slice left, Slice right)
    {
        final long leftUpper = getUpper(left);
        final long rightLower = getLower(right);
        if (abs(leftUpper) == abs(rightLower)) {
            return signum(leftUpper) != signum(rightLower);
        }
        final long leftLower = getLower(left);
        final long rightUpper = getUpper(right);
        if (abs(rightUpper) == abs(leftLower)) {
            return signum(rightUpper) != signum(leftLower);
        }
        return false;
    }

    public static boolean strictlyLeft(Slice left, Slice right)
    {
        final long leftUpper = getUpper(left);
        final long rightLower = getLower(right);
        if (abs(leftUpper) > abs(rightLower)) {
            return false;
        }
        else if (abs(leftUpper) < abs(rightLower)) {
            return true;
        }
        else {
            return leftUpper < 0 || rightLower < 0;
        }
    }

    public static Slice combine(Slice left, Slice right)
    {
        final long lower;
        if (compareLower(left, right) <= 0) {
            lower = getLower(left);
        }
        else {
            lower = getLower(right);
        }
        final long upper;
        if (compareUpper(left, right) >= 0) {
            upper = getUpper(left);
        }
        else {
            upper = getUpper(right);
        }
        return createTSRangeSlice(lower, upper);
    }

    public static boolean contains(Slice left, Slice right)
    {
        if (right.equals(empty())) {
            return true;
        }
        final int lowerCompare = compareLower(left, right);
        final int upperCompare = compareUpper(left, right);
        return lowerCompare <= 0 && upperCompare >= 0;
    }
}
