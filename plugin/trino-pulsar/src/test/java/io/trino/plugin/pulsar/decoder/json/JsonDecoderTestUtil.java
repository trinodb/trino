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
package io.trino.plugin.pulsar.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import io.trino.plugin.pulsar.decoder.DecoderTestUtil;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TestUtil for JsonDecoder
 */
public class JsonDecoderTestUtil
            extends DecoderTestUtil
{
    public JsonDecoderTestUtil()
    {
        super();
    }

    @Override
    public void checkPrimitiveValue(Object actual, Object expected)
    {
        assertTrue(expected instanceof JsonNode);
        if (actual == null || null == expected) {
            assertNull(expected);
            assertNull(actual);
        }
        else if (actual instanceof CharSequence) {
            assertEquals(actual.toString(), ((JsonNode) expected).asText());
        }
        else if (actual instanceof SqlVarbinary) {
            try {
                assertEquals(((SqlVarbinary) actual).getBytes(), ((JsonNode) expected).binaryValue());
            }
            catch (IOException e) {
                fail(format("JsonNode %s formate binary Value failed", ((JsonNode) expected).getNodeType().name()));
            }
        }
        else if (isIntegralType(actual)) {
            assertEquals(((Number) actual).longValue(), ((JsonNode) expected).asLong());
        }
        else if (isRealType(actual)) {
            assertEquals((Number) actual, (JsonNode) expected);
        }
        else {
            assertEquals(actual, expected);
        }
    }

    @Override
    public void checkMapValues(Block block, Type type, Object value)
    {
        assertNotNull("Type is null", type);
        assertTrue("Unexpected type", type instanceof MapType);
        assertTrue("Unexpected key type", ((MapType) type).getKeyType() instanceof VarcharType);
        assertNotNull("Block is null", block);
        assertNotNull("Value is null", value);

        assertTrue("map node isn't ObjectNode type", value instanceof ObjectNode);

        ObjectNode expected = (ObjectNode) value;

        Iterator<Map.Entry<String, JsonNode>> fields = expected.fields();

        assertEquals(block.getPositionCount(), Iterators.size(expected.fields()) * 2);
        Type valueType = ((MapType) type).getValueType();
        if (valueType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(Iterators.any(fields, entry -> entry.getKey().equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block arrayBlock = block.getSingleValueBlock(index + 1); //.getObject(index + 1, Block.class);
                checkArrayValues(arrayBlock, valueType, expected.get(actualKey));
            }
        }
        else if (valueType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(Iterators.any(fields, entry -> entry.getKey().equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block mapBlock = block.getSingleValueBlock(index + 1); //.getObject(index + 1, Block.class);
                checkMapValues(mapBlock, valueType, expected.get(actualKey));
            }
        }
        else if (valueType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(Iterators.any(fields, entry -> entry.getKey().equals(actualKey)));

                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block rowBlock = block.getSingleValueBlock(index + 1); //.getObject(index + 1, Block.class);
                checkRowValues(rowBlock, valueType, expected.get(actualKey));
            }
        }
        else {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                Map.Entry<String, JsonNode> entry = Iterators.tryFind(fields, e -> e.getKey().equals(actualKey)).get();
                assertNotNull(entry);
                assertNotNull(entry.getKey());
                checkPrimitiveValue(getObjectValue(valueType, block, index + 1), entry.getValue());
            }
        }
    }

    @Override
    public void checkRowValues(Block block, Type type, Object value)
    {
        assertNotNull("Type is null", type);
        assertTrue("Unexpected type", type instanceof RowType);
        assertNotNull("Block is null", block);
        assertNotNull("Value is null", value);

        ObjectNode record = (ObjectNode) value;
        RowType rowType = (RowType) type;
        assertEquals("Json field size mismatch", Iterators.size(record.fields()), rowType.getFields().size());
        assertEquals("Trino type field size mismatch", block.getPositionCount(), rowType.getFields().size());
        for (int fieldIndex = 0; fieldIndex < rowType.getFields().size(); fieldIndex++) {
            RowType.Field rowField = rowType.getFields().get(fieldIndex);
            Object expectedValue = record.get(rowField.getName().get());
            if (block.isNull(fieldIndex)) {
                assertNull(expectedValue);
                continue;
            }
            checkField(block, rowField.getType(), fieldIndex, expectedValue);
        }
    }

    @Override
    public void checkArrayValues(Block block, Type type, Object value)
    {
        assertNotNull("Type is null", type);
        assertTrue("Unexpected type", type instanceof ArrayType);
        assertNotNull("Block is null", block);
        assertNotNull("Value is null", value);

        assertTrue("Array node isn't ArrayNode type", value instanceof ArrayNode);
        ArrayNode arrayNode = (ArrayNode) value;

        assertEquals(block.getPositionCount(), arrayNode.size());
        Type elementType = ((ArrayType) type).getElementType();
        if (elementType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(arrayNode.get(index));
                    continue;
                }
                Block arrayBlock = block.getSingleValueBlock(index); //.getObject(index, Block.class);
                checkArrayValues(arrayBlock, elementType, arrayNode.get(index));
            }
        }
        else if (elementType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(arrayNode.get(index));
                    continue;
                }
                Block mapBlock = block.getSingleValueBlock(index); //.getObject(index, Block.class);
                checkMapValues(mapBlock, elementType, arrayNode.get(index));
            }
        }
        else if (elementType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(arrayNode.get(index));
                    continue;
                }
                Block rowBlock = block.getSingleValueBlock(index); //.getObject(index, Block.class);
                checkRowValues(rowBlock, elementType, arrayNode.get(index));
            }
        }
        else {
            for (int index = 0; index < block.getPositionCount(); index++) {
                checkPrimitiveValue(getObjectValue(elementType, block, index), arrayNode.get(index));
            }
        }
    }
}
