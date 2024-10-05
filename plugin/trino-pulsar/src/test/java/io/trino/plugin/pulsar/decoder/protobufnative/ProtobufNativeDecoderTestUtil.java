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
package io.trino.plugin.pulsar.decoder.protobufnative;

import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.EnumValue;
import io.trino.plugin.pulsar.decoder.DecoderTestUtil;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TestUtil for ProtobufNativeDecoder.
 */
public class ProtobufNativeDecoderTestUtil
        extends DecoderTestUtil
{
    public ProtobufNativeDecoderTestUtil()
    {
        super();
    }

    @Override
    public void checkPrimitiveValue(Object actual, Object expected)
    {
        if (actual == null || expected == null) {
            assertNull(expected);
            assertNull(actual);
        }
        else if (actual instanceof CharSequence) {
            assertTrue(expected instanceof CharSequence || expected instanceof EnumValue);
            assertEquals(actual.toString(), expected.toString());
            if (expected instanceof EnumValue) {
                assertEquals(actual.toString(), ((EnumValue) expected).getName());
            }
            else if (expected instanceof CharSequence) {
                assertEquals(actual.toString(), expected.toString());
            }
        }
        else if (actual instanceof SqlVarbinary) {
            if (actual instanceof ByteString) {
                assertEquals(((SqlVarbinary) actual).getBytes(), ((ByteString) expected).toByteArray());
            }
            else if (expected instanceof byte[]) {
                assertEquals(((SqlVarbinary) actual).getBytes(), expected);
            }
            else {
                fail(format("Unexpected value type %s", actual.getClass()));
            }
        }
        else if (isIntegralType(actual) && isIntegralType(expected)) {
            assertEquals(((Number) actual).longValue(), ((Number) expected).longValue());
        }
        else if (isRealType(actual) && isRealType(expected)) {
            assertEquals((Number) actual, (Number) expected);
        }
        else {
            assertEquals(actual, expected);
        }
    }

    @Override
    public void checkArrayValues(Block block, Type type, Object value)
    {
        assertNotNull("Type is null", type);
        assertTrue("Unexpected type", type instanceof ArrayType);
        assertNotNull("Block is null", block);
        assertNotNull("Value is null", value);

        List<?> list = (List<?>) value;

        assertEquals(block.getPositionCount(), list.size());
        Type elementType = ((ArrayType) type).getElementType();
        if (elementType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block arrayBlock = block.getSingleValueBlock(index); //.getObject(index, Block.class);
                checkArrayValues(arrayBlock, elementType, list.get(index));
            }
        }
        else if (elementType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block mapBlock = block.getSingleValueBlock(index); //.getObject(index, Block.class);
                checkMapValues(mapBlock, elementType, list.get(index));
            }
        }
        else if (elementType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block rowBlock = block.getSingleValueBlock(index); //.getObject(index, Block.class);
                checkRowValues(rowBlock, elementType, list.get(index));
            }
        }
        else {
            for (int index = 0; index < block.getPositionCount(); index++) {
                checkPrimitiveValue(getObjectValue(elementType, block, index), list.get(index));
            }
        }
    }

    @Override
    public void checkMapValues(Block block, Type type, Object value)
    {
        assertNotNull("Type is null", type);
        assertTrue("Unexpected type", type instanceof MapType);
        assertNotNull("Block is null", block);
        assertNotNull("Value is null", value);

        Map<?, ?> expected = PulsarProtobufNativeColumnDecoder.parseProtobufMap(value);

        assertEquals(block.getPositionCount(), expected.size() * 2);
        Type valueType = ((MapType) type).getValueType();
        //protobuf3 keyType only support integral or string type
        Type keyType = ((MapType) type).getKeyType();

        //check value
        if (valueType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                Object actualKey = getObjectValue(keyType, block, index);
                assertTrue(expected.keySet().stream().anyMatch(e -> e.equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block arrayBlock = block.getSingleValueBlock(index + 1); //.getObject(index + 1, Block.class);
                Object keyValue = expected.entrySet().stream().filter(e -> e.getKey().equals(actualKey)).findFirst().get().getValue();
                checkArrayValues(arrayBlock, valueType, keyValue);
            }
        }
        else if (valueType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                Object actualKey = getObjectValue(keyType, block, index);
                assertTrue(expected.keySet().stream().anyMatch(e -> e.equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block mapBlock = block.getSingleValueBlock(index + 1); //.getObject(index + 1, Block.class);
                Object keyValue = expected.entrySet().stream().filter(e -> e.getKey().equals(actualKey)).findFirst().get().getValue();
                checkMapValues(mapBlock, valueType, keyValue);
            }
        }
        else if (valueType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                Object actualKey = getObjectValue(keyType, block, index);
                assertTrue(expected.keySet().stream().anyMatch(e -> e.equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block rowBlock = block.getSingleValueBlock(index + 1); //.getObject(index + 1, Block.class);
                Object keyValue = expected.entrySet().stream().filter(e -> e.getKey().equals(actualKey)).findFirst().get().getValue();
                checkRowValues(rowBlock, valueType, keyValue);
            }
        }
        else {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                Object actualKey = getObjectValue(keyType, block, index);
                assertTrue(expected.keySet().stream().anyMatch(e -> e.equals(actualKey)));
                Object keyValue = expected.entrySet().stream().filter(e -> e.getKey().equals(actualKey)).findFirst().get().getValue();
                checkPrimitiveValue(getObjectValue(valueType, block, index + 1), keyValue);
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

        DynamicMessage record = (DynamicMessage) value;
        RowType rowType = (RowType) type;
        assertEquals("Protobuf field size mismatch", record.getAllFields().size(), rowType.getFields().size());
        assertEquals("Trino type field size mismatch", block.getPositionCount(), rowType.getFields().size());
        for (int fieldIndex = 0; fieldIndex < rowType.getFields().size(); fieldIndex++) {
            RowType.Field rowField = rowType.getFields().get(fieldIndex);
            Object expectedValue =
                    record.getField(((DynamicMessage) value).getDescriptorForType().findFieldByName(rowField.getName().get()));

            if (block.isNull(fieldIndex)) {
                assertNull(expectedValue);
                continue;
            }
            checkField(block, rowField.getType(), fieldIndex, expectedValue);
        }
    }
}
