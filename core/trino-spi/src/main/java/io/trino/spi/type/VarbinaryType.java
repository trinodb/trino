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
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;

import java.util.Optional;

public final class VarbinaryType
        extends AbstractVariableWidthType
{
    public static final String NAME = "varbinary";

    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = TypeOperatorDeclaration.builder(Slice.class)
            .addOperators(DEFAULT_READ_OPERATORS)
            .addOperators(DEFAULT_COMPARABLE_OPERATORS)
            .addOperators(DEFAULT_ORDERING_OPERATORS)
            .build();

    public static final VarbinaryType VARBINARY = new VarbinaryType();

    private VarbinaryType()
    {
        super(new TypeDescriptor(NAME), Slice.class);
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

        return new SqlVarbinary(getSlice(block, position).getBytes());
    }

    @Override
    public Optional<Object> getPreviousValue(Object object)
    {
        Slice value = (Slice) object;
        // The values are ordered as unsigned bytes and their length is unbounded, so the greatest lesser value
        // exists only when the value ends with a lowest byte: nothing sorts between a value and the same value
        // without its trailing zero byte.
        if (value.length() == 0 || value.getByte(value.length() - 1) != 0) {
            return Optional.empty();
        }
        return Optional.of(value.copy(0, value.length() - 1));
    }

    @Override
    public Optional<Object> getNextValue(Object object)
    {
        Slice value = (Slice) object;
        // nothing sorts between a value and the same value with a zero byte appended
        Slice next = Slices.allocate(value.length() + 1);
        next.setBytes(0, value);
        return Optional.of(next);
    }

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
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(value, offset, length);
    }

    @Override
    public boolean equals(Object other)
    {
        return other == VARBINARY;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
