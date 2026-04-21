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

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.json.ir.IrJsonPath;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.TypeSignature;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class JsonPath2016Type
        extends AbstractVariableWidthType
{
    public static final String NAME = "JsonPath2016";

    private final JsonCodec<IrJsonPath> jsonPathCodec;

    public JsonPath2016Type(JsonCodec<IrJsonPath> jsonPathCodec)
    {
        super(new TypeSignature(NAME), IrJsonPath.class);
        this.jsonPathCodec = requireNonNull(jsonPathCodec, "jsonPathCodec is null");
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
        return jsonPathCodec.fromJson(valueBlock.getSlice(valuePosition).getInput());
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        String json = jsonPathCodec.toJson((IrJsonPath) value);
        Slice bytes = utf8Slice(json);
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(bytes);
    }
}
