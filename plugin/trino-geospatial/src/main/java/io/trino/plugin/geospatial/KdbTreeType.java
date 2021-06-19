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
import io.trino.geospatial.KdbTree;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.TypeSignature;

import static io.airlift.slice.Slices.utf8Slice;

public final class KdbTreeType
        extends AbstractVariableWidthType
{
    public static final KdbTreeType KDB_TREE = new KdbTreeType();
    public static final String NAME = "KdbTree";

    private KdbTreeType()
    {
        // The KDB tree type should be KdbTree but can not be since KdbTree is in
        // both the plugin class loader and the system class loader.  This was done
        // so the plan optimizer can process geo spatial joins.
        super(new TypeSignature(NAME), Object.class);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return getObject(block, position);
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
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        String json = KdbTreeUtils.toJson(((KdbTree) value));
        Slice bytes = utf8Slice(json);
        blockBuilder.writeBytes(bytes, 0, bytes.length()).closeEntry();
    }

    @Override
    public Object getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        Slice bytes = block.getSlice(position, 0, block.getSliceLength(position));
        KdbTree kdbTree = KdbTreeUtils.fromJson(bytes.toStringUtf8());
        return kdbTree;
    }
}
