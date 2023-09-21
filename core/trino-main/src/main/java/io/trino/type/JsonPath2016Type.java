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

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.trino.block.BlockJsonSerde;
import io.trino.json.ir.IrJsonPath;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import static io.airlift.slice.Slices.utf8Slice;

public class JsonPath2016Type
        extends AbstractVariableWidthType
{
    public static final String NAME = "JsonPath2016";

    private final JsonCodec<IrJsonPath> jsonPathCodec;

    public JsonPath2016Type(TypeDeserializer typeDeserializer, BlockEncodingSerde blockEncodingSerde)
    {
        super(new TypeSignature(NAME), IrJsonPath.class);
        this.jsonPathCodec = getCodec(typeDeserializer, blockEncodingSerde);
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

        Slice bytes = block.getSlice(position, 0, block.getSliceLength(position));
        return jsonPathCodec.fromJson(bytes.toStringUtf8());
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        String json = jsonPathCodec.toJson((IrJsonPath) value);
        Slice bytes = utf8Slice(json);
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(bytes);
    }

    private static JsonCodec<IrJsonPath> getCodec(TypeDeserializer typeDeserializer, BlockEncodingSerde blockEncodingSerde)
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)));
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, typeDeserializer,
                Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde)));
        return new JsonCodecFactory(provider).jsonCodec(IrJsonPath.class);
    }
}
