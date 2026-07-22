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

import io.airlift.slice.XxHash64;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.ScalarOperator;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Optional;

import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.SORT_KEY_PREFIX_BATCH_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.SORT_KEY_PREFIX_BATCH_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.SORT_KEY_PREFIX_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.SORT_KEY_PREFIX_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.isNaN;
import static java.lang.Double.longBitsToDouble;
import static java.lang.invoke.MethodHandles.lookup;

public final class DoubleType
        extends AbstractType
        implements FixedWidthType
{
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    public static final String NAME = "double";
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = TypeOperatorDeclaration.builder(double.class)
            .addOperators(extractOperatorDeclaration(DoubleType.class, lookup(), double.class))
            .sortKeyPrefixExact(true)
            .build();
    private static final VarHandle DOUBLE_HANDLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.LITTLE_ENDIAN);

    public static final DoubleType DOUBLE = new DoubleType();

    private DoubleType()
    {
        super(new TypeDescriptor(NAME), double.class, LongArrayBlock.class);
    }

    @Override
    public int getFixedSize()
    {
        return Double.BYTES;
    }

    @Override
    public String getDisplayName()
    {
        return NAME;
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
        return getDouble(block, position);
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return read((LongArrayBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        ((LongArrayBlockBuilder) blockBuilder).writeLong(doubleToLongBits(value));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new LongArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Double.BYTES));
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new LongArrayBlockBuilder(null, positionCount);
    }

    @Override
    public int getFlatFixedSize()
    {
        return Double.BYTES;
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == DOUBLE;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public Optional<Range> getRange()
    {
        // The range for double is undefined because NaN is a special value that
        // is *not* in any reasonable definition of a range for this type.
        return Optional.empty();
    }

    @ScalarOperator(READ_VALUE)
    private static double read(@BlockPosition LongArrayBlock block, @BlockIndex int position)
    {
        return longBitsToDouble(block.getLong(position));
    }

    @ScalarOperator(READ_VALUE)
    private static double readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        return (double) DOUBLE_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            double value,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        DOUBLE_HANDLE.set(fixedSizeSlice, fixedSizeOffset, value);
    }

    @SuppressWarnings("FloatingPointEquality")
    @ScalarOperator(EQUAL)
    private static boolean equalOperator(double left, double right)
    {
        return left == right;
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(double value)
    {
        if (value == 0) {
            value = 0;
        }
        return AbstractLongType.hash(doubleToLongBits(value));
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64(double value)
    {
        if (value == 0) {
            value = 0;
        }
        return XxHash64.hash(doubleToLongBits(value));
    }

    @SuppressWarnings("FloatingPointEquality")
    @ScalarOperator(IDENTICAL)
    private static boolean identical(double left, @IsNull boolean leftNull, double right, @IsNull boolean rightNull)
    {
        if (leftNull || rightNull) {
            return leftNull == rightNull;
        }

        if (isNaN(left) && isNaN(right)) {
            return true;
        }
        return left == right;
    }

    @ScalarOperator(SORT_KEY_PREFIX_UNORDERED_LAST)
    private static long sortKeyPrefixUnorderedLastOperator(double value)
    {
        if (isNaN(value)) {
            // above all normal keys; no non-NaN double encodes to this key
            return -1;
        }
        return sortKeyPrefix(value);
    }

    @ScalarOperator(SORT_KEY_PREFIX_UNORDERED_FIRST)
    private static long sortKeyPrefixUnorderedFirstOperator(double value)
    {
        if (isNaN(value)) {
            // below all normal keys; no non-NaN double encodes to this key
            return 0;
        }
        return sortKeyPrefix(value);
    }

    private static long sortKeyPrefix(double value)
    {
        long bits = doubleToLongBits(value);
        if (bits < 0) {
            return ~bits;
        }
        return bits ^ Long.MIN_VALUE;
    }

    @ScalarOperator(SORT_KEY_PREFIX_BATCH_UNORDERED_LAST)
    private static void sortKeyPrefixBatchUnorderedLastOperator(LongArrayBlock block, long[] prefixes, int positionCount)
    {
        sortKeyPrefixBatch(block, prefixes, positionCount, -1);
    }

    @ScalarOperator(SORT_KEY_PREFIX_BATCH_UNORDERED_FIRST)
    private static void sortKeyPrefixBatchUnorderedFirstOperator(LongArrayBlock block, long[] prefixes, int positionCount)
    {
        sortKeyPrefixBatch(block, prefixes, positionCount, 0);
    }

    /**
     * Vectorized variant of {@link #sortKeyPrefix}: negative values map to {@code ~bits} and
     * others to {@code bits ^ Long.MIN_VALUE}, expressed branch-free as {@code bits ^ ((bits >> 63)
     * | Long.MIN_VALUE)}. NaNs (including non-canonical bit patterns) are blended to the flavor's
     * key, matching the scalar operator's {@code isNaN} check.
     */
    private static void sortKeyPrefixBatch(LongArrayBlock block, long[] prefixes, int positionCount, long nanKey)
    {
        long[] values = block.getRawValues();
        int offset = block.getRawValuesOffset();
        int upperBound = LONG_SPECIES.loopBound(positionCount);
        int i = 0;
        for (; i < upperBound; i += LONG_SPECIES.length()) {
            LongVector bits = LongVector.fromArray(LONG_SPECIES, values, offset + i);
            LongVector transformed = bits.lanewise(VectorOperators.XOR, bits.lanewise(VectorOperators.ASHR, 63).lanewise(VectorOperators.OR, Long.MIN_VALUE));
            VectorMask<Long> nan = bits.lanewise(VectorOperators.AND, ~Long.MIN_VALUE).compare(VectorOperators.GT, 0x7FF0_0000_0000_0000L);
            transformed.blend(nanKey, nan).intoArray(prefixes, i);
        }
        for (; i < positionCount; i++) {
            long bits = values[offset + i];
            if ((bits & ~Long.MIN_VALUE) > 0x7FF0_0000_0000_0000L) {
                prefixes[i] = nanKey;
            }
            else {
                prefixes[i] = bits ^ ((bits >> 63) | Long.MIN_VALUE);
            }
        }
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonUnorderedLastOperator(double left, double right)
    {
        return compare(left, right);
    }

    @ScalarOperator(COMPARISON_UNORDERED_FIRST)
    private static long comparisonUnorderedFirstOperator(double left, double right)
    {
        // Double compare puts NaN last, so we must handle NaNs manually
        if (isNaN(left) && isNaN(right)) {
            return 0;
        }
        if (isNaN(left)) {
            return -1;
        }
        if (isNaN(right)) {
            return 1;
        }

        return compare(left, right);
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(double left, double right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(double left, double right)
    {
        return left <= right;
    }

    private static int compare(double left, double right)
    {
        if (left == right) { // Double.compare considers 0.0 and -0.0 different from each other
            return 0;
        }

        return Double.compare(left, right);
    }
}
