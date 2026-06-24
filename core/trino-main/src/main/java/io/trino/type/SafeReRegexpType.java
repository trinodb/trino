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
package io.trino.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.TypeDescriptor;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class SafeReRegexpType
        extends AbstractVariableWidthType
{
    public static final String NAME = "SafeReRegExp";
    public static final TypeDescriptor SAFE_RE_REGEXP_SIGNATURE = new TypeDescriptor(NAME);
    public static final SafeReRegexpType SAFE_RE_REGEXP = new SafeReRegexpType();

    public SafeReRegexpType()
    {
        super(SAFE_RE_REGEXP_SIGNATURE, SafeReRegexp.class);
    }

    @Override
    public String getDisplayName()
    {
        return NAME;
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSlice(valuePosition).toStringUtf8();
    }

    @Override
    public Object getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        Slice pattern = valueBlock.getSlice(valuePosition);
        try {
            return new SafeReRegexp(pattern);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        Slice pattern = Slices.utf8Slice(((SafeReRegexp) value).pattern());
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(pattern);
    }
}
