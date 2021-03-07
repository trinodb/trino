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
import io.airlift.slice.XxHash64;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.ScalarOperator;

import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.Slices.sliceRepresentation;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Collections.singletonList;

public final class VarcharType
        extends AbstractVariableWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(VarcharType.class, lookup(), Slice.class);

    public static final int UNBOUNDED_LENGTH = Integer.MAX_VALUE;
    public static final int MAX_LENGTH = Integer.MAX_VALUE - 1;
    public static final VarcharType VARCHAR = new VarcharType(UNBOUNDED_LENGTH);

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
        return new VarcharType(length);
    }

    public static TypeSignature getParametrizedVarcharSignature(String param)
    {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.typeVariable(param));
    }

    private final int length;

    private VarcharType(int length)
    {
        super(
                new TypeSignature(
                        StandardTypes.VARCHAR,
                        singletonList(TypeSignatureParameter.numericParameter((long) length))),
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
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
        if (!isUnbounded() && countCodePoints(slice) > length) {
            throw new IllegalArgumentException(format("Character count exceeds length limit %s: %s", length, sliceRepresentation(slice)));
        }
        return slice.toStringUtf8();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
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
        if (length > 100) {
            // The max/min values may be materialized in the plan, so we don't want them to be too large.
            // Range comparison against large values are usually nonsensical, too, so no need to support them
            // beyond a certain size. They specific choice above is arbitrary and can be adjusted if needed.
            return Optional.empty();
        }

        int codePointSize = SliceUtf8.lengthOfCodePoint(MAX_CODE_POINT);

        Slice max = Slices.allocate(codePointSize * length);
        int position = 0;
        for (int i = 0; i < length; i++) {
            position += SliceUtf8.setCodePointAt(MAX_CODE_POINT, max, position);
        }

        return Optional.of(new Range(Slices.EMPTY_SLICE, max));
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getSliceLength(position));
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
        blockBuilder.writeBytes(value, offset, length).closeEntry();
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

        return Objects.equals(this.length, other.length);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(length);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Slice left, Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        int leftLength = leftBlock.getSliceLength(leftPosition);
        int rightLength = rightBlock.getSliceLength(rightPosition);
        if (leftLength != rightLength) {
            return false;
        }
        return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, leftLength);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Block block, @BlockIndex int position)
    {
        return block.hash(position, 0, block.getSliceLength(position));
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(Slice left, Slice right)
    {
        return left.compareTo(right);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        int leftLength = leftBlock.getSliceLength(leftPosition);
        int rightLength = rightBlock.getSliceLength(rightPosition);
        return leftBlock.compareTo(leftPosition, 0, leftLength, rightBlock, rightPosition, 0, rightLength);
    }
}
