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
package io.prestosql.decoder.avro;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AvroDecoderTestUtil
{
    private AvroDecoderTestUtil() {}

    public static void checkPrimitiveValue(Object actual, Object expected)
    {
        if (actual == null || expected == null) {
            assertNull(expected);
            assertNull(actual);
        }
        else if (actual instanceof CharSequence) {
            assertTrue(expected instanceof CharSequence || expected instanceof GenericEnumSymbol);
            assertEquals(actual.toString(), expected.toString());
        }
        else if (actual instanceof SqlVarbinary) {
            if (expected instanceof GenericFixed) {
                assertEquals(((SqlVarbinary) actual).getBytes(), ((GenericFixed) expected).bytes());
            }
            else if (expected instanceof ByteBuffer) {
                assertEquals(((SqlVarbinary) actual).getBytes(), ((ByteBuffer) expected).array());
            }
            else {
                fail(format("Unexpected value type %s", actual.getClass()));
            }
        }
        else if (isIntegralType(actual) && isIntegralType(expected)) {
            assertEquals(((Number) actual).longValue(), ((Number) expected).longValue());
        }
        else if (isRealType(actual) && isRealType(expected)) {
            assertEquals(((Number) actual).doubleValue(), ((Number) expected).doubleValue());
        }
        else {
            assertEquals(actual, expected);
        }
    }

    public static boolean isIntegralType(Object value)
    {
        return value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte;
    }

    public static boolean isRealType(Object value)
    {
        return value instanceof Float || value instanceof Double;
    }

    public static Object getObjectValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return type.getObjectValue(SESSION, block, position);
    }

    public static void checkArrayValues(Block block, Type type, Object value)
    {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof ArrayType, "Unexpected type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");

        List<?> list = (List<?>) value;

        assertEquals(block.getPositionCount(), list.size());
        Type elementType = ((ArrayType) type).getElementType();
        if (elementType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block arrayBlock = block.getObject(index, Block.class);
                checkArrayValues(arrayBlock, elementType, list.get(index));
            }
        }
        else if (elementType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block mapBlock = block.getObject(index, Block.class);
                checkMapValues(mapBlock, elementType, list.get(index));
            }
        }
        else if (elementType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block rowBlock = block.getObject(index, Block.class);
                checkRowValues(rowBlock, elementType, list.get(index));
            }
        }
        else {
            for (int index = 0; index < block.getPositionCount(); index++) {
                checkPrimitiveValue(getObjectValue(elementType, block, index), list.get(index));
            }
        }
    }

    public static void checkMapValues(Block block, Type type, Object value)
    {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof MapType, "Unexpected type");
        assertTrue(((MapType) type).getKeyType() instanceof VarcharType, "Unexpected key type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");

        Map<String, ?> expected = (Map<String, ?>) value;

        assertEquals(block.getPositionCount(), expected.size() * 2);
        Type valueType = ((MapType) type).getValueType();
        if (valueType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(expected.containsKey(actualKey));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block arrayBlock = block.getObject(index + 1, Block.class);
                checkArrayValues(arrayBlock, valueType, expected.get(actualKey));
            }
        }
        else if (valueType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(expected.containsKey(actualKey));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block mapBlock = block.getObject(index + 1, Block.class);
                checkMapValues(mapBlock, valueType, expected.get(actualKey));
            }
        }
        else if (valueType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(expected.containsKey(actualKey));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block rowBlock = block.getObject(index + 1, Block.class);
                checkRowValues(rowBlock, valueType, expected.get(actualKey));
            }
        }
        else {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(expected.containsKey(actualKey));
                checkPrimitiveValue(getObjectValue(valueType, block, index + 1), expected.get(actualKey));
            }
        }
    }

    public static void checkRowValues(Block block, Type type, Object value)
    {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof RowType, "Unexpected type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");

        GenericRecord record = (GenericRecord) value;
        RowType rowType = (RowType) type;
        assertEquals(record.getSchema().getFields().size(), rowType.getFields().size(), "Avro field size mismatch");
        assertEquals(block.getPositionCount(), rowType.getFields().size(), "Presto type field size mismatch");
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

    private static void checkField(Block actualBlock, Type type, int position, Object expectedValue)
    {
        assertNotNull(type, "Type is null");
        assertNotNull(actualBlock, "actualBlock is null");
        assertTrue(!actualBlock.isNull(position));
        assertNotNull(expectedValue, "expectedValue is null");

        if (type instanceof ArrayType) {
            checkArrayValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        }
        else if (type instanceof MapType) {
            checkMapValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        }
        else if (type instanceof RowType) {
            checkRowValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        }
        else {
            checkPrimitiveValue(getObjectValue(type, actualBlock, position), expectedValue);
        }
    }
}
