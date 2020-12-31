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
import io.prestosql.operator.scalar.JsonPath;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.AbstractVariableWidthType;
import io.prestosql.spi.type.TypeSignature;

public class JsonPathType
        extends AbstractVariableWidthType
{
    public static final JsonPathType JSON_PATH = new JsonPathType();
    public static final String NAME = "JsonPath";

    private JsonPathType()
    {
        super(new TypeSignature(NAME), JsonPath.class);
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

        return new JsonPath(block.getSlice(position, 0, block.getSliceLength(position)).toStringUtf8());
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        Slice pattern = Slices.utf8Slice(((JsonPath) value).pattern());
        blockBuilder.writeBytes(pattern, 0, pattern.length()).closeEntry();
    }
}
