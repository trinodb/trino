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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.trino.operator.scalar.json.JsonInputConversionException;
import io.trino.operator.scalar.json.JsonOutputConversionException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;

import java.io.IOException;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.json.JsonInputErrorNode.JSON_ERROR;
import static io.trino.util.JsonUtil.createJsonFactory;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Json2016Type
        extends AbstractVariableWidthType
{
    public static final Json2016Type JSON_2016 = new Json2016Type();
    private static final JsonFactory JSON_FACTORY = createJsonFactory();

    static {
        // Changes factory. Necessary for JsonParser.readValueAsTree to work.
        new ObjectMapper(JSON_FACTORY);
    }

    public Json2016Type()
    {
        super(new TypeSignature(StandardTypes.JSON_2016), JsonNode.class);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return getObject(block, position);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        String json = valueBlock.getSlice(valuePosition).toStringUtf8();
        if (json.equals(JSON_ERROR.toString())) {
            return JSON_ERROR;
        }
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            return parser.readValueAsTree();
        }
        catch (IOException e) {
            throw new JsonInputConversionException(e);
        }
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        byte[] json;
        if (value == JSON_ERROR) {
            json = JSON_ERROR.toString().getBytes(UTF_8);
        }
        else {
            ByteArrayBuilder builder = new ByteArrayBuilder();
            try (JsonGenerator generator = JSON_FACTORY.createGenerator(builder)) {
                generator.writeObject(value);
            }
            catch (IOException e) {
                throw new JsonOutputConversionException(e);
            }
            json = builder.toByteArray();
        }
        Slice bytes = wrappedBuffer(json);
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(bytes);
    }
}
