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
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.fromCodePoints;
import static io.trino.spi.type.CodePoints.nextCodePoint;
import static io.trino.spi.type.CodePoints.previousCodePoint;
import static io.trino.spi.type.CodePoints.tryCodePoints;
import static io.trino.spi.type.Slices.sliceRepresentation;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MIN_CODE_POINT;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

public final class VarcharType
        extends AbstractVariableWidthType
{
    public static final String NAME = "varchar";

    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = TypeOperatorDeclaration.builder(Slice.class)
            .addOperators(DEFAULT_READ_OPERATORS)
            .addOperators(DEFAULT_COMPARABLE_OPERATORS)
            .addOperators(DEFAULT_ORDERING_OPERATORS)
            .build();

    public static final int UNBOUNDED_LENGTH = Integer.MAX_VALUE;
    public static final int MAX_LENGTH = Integer.MAX_VALUE - 1;
    public static final VarcharType VARCHAR = new VarcharType(UNBOUNDED_LENGTH);

    // The range bounds, as well as the values adjacent to a given value, may be materialized in the plan, so we
    // don't want them to be too large. Range comparison against large values is usually nonsensical, too, so there
    // is no need to support them beyond a certain size. The specific choice here is arbitrary and can be adjusted
    // if needed.
    private static final int MAX_MATERIALIZED_VALUE_LENGTH = 100;

    private static final VarcharType[] CACHED_INSTANCES = new VarcharType[128];

    static {
        for (int i = 0; i < CACHED_INSTANCES.length; i++) {
            CACHED_INSTANCES[i] = new VarcharType(i);
        }
    }

    public static VarcharType createUnboundedVarcharType()
    {
        return VARCHAR;
    }

    public static VarcharType createVarcharType(int length)
    {
        if (length > MAX_LENGTH || length < 0) {
            // Use createUnboundedVarcharType for unbounded VARCHAR.
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        if (length < CACHED_INSTANCES.length) {
            return CACHED_INSTANCES[length];
        }
        return new VarcharType(length);
    }

    private final int length;
    private volatile Optional<Range> range;

    private VarcharType(int length)
    {
        super(
                new TypeDescriptor(
                        NAME,
                        singletonList(TypeParameter.numericParameter(length))),
                Slice.class);

        if (length < 0) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        this.length = length;
    }

    public Optional<Integer> getLength()
    {
        if (isUnbounded()) {
            return Optional.empty();
        }
        return Optional.of(length);
    }

    public int getBoundedLength()
    {
        if (isUnbounded()) {
            throw new IllegalStateException("Cannot get size of unbounded VARCHAR.");
        }
        return length;
    }

    public boolean isUnbounded()
    {
        return length == UNBOUNDED_LENGTH;
    }

    @Override
    public String getDisplayName()
    {
        return NAME + (isUnbounded() ? "" : "(" + length + ")");
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

        Slice slice = getSlice(block, position);
        if (!isUnbounded() && countCodePoints(slice) > length) {
            throw new IllegalArgumentException(format("Character count exceeds length limit %s: %s", length, sliceRepresentation(slice)));
        }
        return slice.toStringUtf8();
    }

    @Override
    public VariableWidthBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(
                blockBuilderStatus,
                expectedEntries,
                getLength()
                        // If bound on length is smaller than EXPECTED_BYTES_PER_ENTRY, use that as expectedBytesPerEntry
                        // The data can take up to 4 bytes per character due to UTF-8 encoding, but we assume it is ASCII and only needs one byte.
                        .map(length -> Math.min(length, EXPECTED_BYTES_PER_ENTRY))
                        .orElse(EXPECTED_BYTES_PER_ENTRY));
    }

    @Override
    public Optional<Range> getRange()
    {
        Optional<Range> range = this.range;
        @SuppressWarnings("OptionalAssignedToNull")
        boolean cachedRangePresent = range != null;
        if (!cachedRangePresent) {
            if (length > MAX_MATERIALIZED_VALUE_LENGTH) {
                range = Optional.empty();
            }
            else {
                int codePointSize = SliceUtf8.lengthOfCodePoint(MAX_CODE_POINT);

                Slice max = Slices.allocate(codePointSize * length);
                int position = 0;
                for (int i = 0; i < length; i++) {
                    position += SliceUtf8.setCodePointAt(MAX_CODE_POINT, max, position);
                }

                range = Optional.of(new Range(Slices.EMPTY_SLICE, max));
            }
            this.range = range;
        }
        return range;
    }

    @Override
    public Optional<Object> getPreviousValue(Object value)
    {
        if (isUnbounded() || length > MAX_MATERIALIZED_VALUE_LENGTH) {
            // The greatest lesser value is padded with the highest code point up to the length of the type, so it
            // does not exist when the length is unbounded, and it is not materialized when the length is large.
            return Optional.empty();
        }
        Optional<int[]> decoded = tryCodePoints((Slice) value);
        if (decoded.isEmpty()) {
            return Optional.empty();
        }
        int[] valueCodePoints = decoded.get();
        if (valueCodePoints.length == 0) {
            // the empty value is the least
            return Optional.empty();
        }
        int lastPosition = valueCodePoints.length - 1;
        if (valueCodePoints[lastPosition] == MIN_CODE_POINT) {
            // nothing sorts between a value and the same value without its trailing lowest code point
            return Optional.of(fromCodePoints(valueCodePoints, 0, lastPosition));
        }
        int[] codePoints = Arrays.copyOf(valueCodePoints, length);
        codePoints[lastPosition] = previousCodePoint(valueCodePoints[lastPosition]);
        Arrays.fill(codePoints, lastPosition + 1, length, MAX_CODE_POINT);
        return Optional.of(fromCodePoints(codePoints));
    }

    @Override
    public Optional<Object> getNextValue(Object value)
    {
        Optional<int[]> decoded = tryCodePoints((Slice) value);
        if (decoded.isEmpty()) {
            return Optional.empty();
        }
        int[] codePoints = decoded.get();
        if (isUnbounded() || codePoints.length < length) {
            // nothing sorts between a value and the same value with the lowest code point appended
            int[] nextCodePoints = Arrays.copyOf(codePoints, codePoints.length + 1);
            nextCodePoints[codePoints.length] = MIN_CODE_POINT;
            return Optional.of(fromCodePoints(nextCodePoints));
        }
        // The value is as long as the type allows, so the least greater value increments the last code point that
        // is not the highest and drops the code points after it.
        for (int position = codePoints.length - 1; position >= 0; position--) {
            if (codePoints[position] != MAX_CODE_POINT) {
                codePoints[position] = nextCodePoint(codePoints[position]);
                return Optional.of(fromCodePoints(codePoints, 0, position + 1));
            }
        }
        return Optional.empty();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSlice(valuePosition);
    }

    public void writeString(BlockBuilder blockBuilder, String value)
    {
        writeSlice(blockBuilder, Slices.utf8Slice(value));
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(value, offset, length);
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

        VarcharType other = (VarcharType) o;
        return this.length == other.length;
    }

    @Override
    public int hashCode()
    {
        return (length * 31) + getClass().hashCode();
    }
}
