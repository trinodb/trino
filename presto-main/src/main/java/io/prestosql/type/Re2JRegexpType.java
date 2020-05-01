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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.AbstractVariableWidthType;
import io.prestosql.spi.type.TypeSignature;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class Re2JRegexpType
        extends AbstractVariableWidthType
{
    public static final String NAME = "Re2JRegExp";
    public static final TypeSignature RE2J_REGEXP_SIGNATURE = new TypeSignature(NAME);

    private final int dfaStatesLimit;
    private final int dfaRetries;

    public Re2JRegexpType(int dfaStatesLimit, int dfaRetries)
    {
        super(RE2J_REGEXP_SIGNATURE, Re2JRegexp.class);
        this.dfaStatesLimit = dfaStatesLimit;
        this.dfaRetries = dfaRetries;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice pattern = block.getSlice(position, 0, block.getSliceLength(position));
        try {
            return new Re2JRegexp(dfaStatesLimit, dfaRetries, pattern);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        Slice pattern = Slices.utf8Slice(((Re2JRegexp) value).pattern());
        blockBuilder.writeBytes(pattern, 0, pattern.length()).closeEntry();
    }
}
