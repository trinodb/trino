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
package io.trino.decoder.avro;

import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class AvroDecoderTestUtil
{
    private AvroDecoderTestUtil() {}

    private static void checkPrimitiveValue(Object actual, Object expected)
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

    private static boolean isIntegralType(Object value)
    {
        return value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte;
    }

    private static boolean isRealType(Object value)
    {
        return value instanceof Float || value instanceof Double;
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
        for (int index = 0; index < block.getPositionCount(); index++) {
            if (block.isNull(index)) {
                assertNull(list.get(index));
                continue;
            }
            if (elementType instanceof ArrayType arrayType) {
                checkArrayValues(arrayType.getObject(block, index), elementType, list.get(index));
            }
            else if (elementType instanceof MapType mapType) {
                checkMapValues(mapType.getObject(block, index), elementType, list.get(index));
            }
            else if (elementType instanceof RowType rowType) {
                checkRowValues(rowType.getObject(block, index), elementType, list.get(index));
            }
            else {
                checkPrimitiveValue(elementType.getObjectValue(SESSION, block, index), list.get(index));
            }
        }
    }

    public static void checkMapValues(SqlMap sqlMap, Type type, Object value)
    {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof MapType, "Unexpected type");
        assertTrue(((MapType) type).getKeyType() instanceof VarcharType, "Unexpected key type");
        assertNotNull(sqlMap, "sqlMap is null");
        assertNotNull(value, "Value is null");

        Map<?, ?> expected = (Map<?, ?>) value;

        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        assertEquals(sqlMap.getSize(), expected.size());
        Type valueType = ((MapType) type).getValueType();
        for (int index = 0; index < sqlMap.getSize(); index++) {
            String actualKey = VARCHAR.getSlice(rawKeyBlock, rawOffset + index).toStringUtf8();
            assertTrue(expected.containsKey(actualKey), "Key not found: %s".formatted(actualKey));
            if (rawValueBlock.isNull(rawOffset + index)) {
                assertNull(expected.get(actualKey));
                continue;
            }
            if (valueType instanceof ArrayType arrayType) {
                checkArrayValues(arrayType.getObject(rawValueBlock, rawOffset + index), valueType, expected.get(actualKey));
            }
            else if (valueType instanceof MapType mapType) {
                checkMapValues(mapType.getObject(rawValueBlock, rawOffset + index), valueType, expected.get(actualKey));
            }
            else if (valueType instanceof RowType rowType) {
                checkRowValues(rowType.getObject(rawValueBlock, rawOffset + index), valueType, expected.get(actualKey));
            }
            else {
                checkPrimitiveValue(valueType.getObjectValue(SESSION, rawValueBlock, rawOffset + index), expected.get(actualKey));
            }
        }
    }

    public static void checkRowValues(SqlRow sqlRow, Type type, Object value)
    {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof RowType, "Unexpected type");
        assertNotNull(sqlRow, "sqlRow is null");
        assertNotNull(value, "Value is null");

        GenericRecord record = (GenericRecord) value;
        RowType rowType = (RowType) type;
        assertEquals(record.getSchema().getFields().size(), rowType.getFields().size(), "Avro field size mismatch");
        assertEquals(sqlRow.getFieldCount(), rowType.getFields().size(), "Trino type field size mismatch");
        int rawIndex = sqlRow.getRawIndex();
        for (int fieldIndex = 0; fieldIndex < rowType.getFields().size(); fieldIndex++) {
            RowType.Field rowField = rowType.getFields().get(fieldIndex);
            Object expectedValue = record.get(rowField.getName().orElseThrow());
            Block fieldBlock = sqlRow.getRawFieldBlock(fieldIndex);
            if (fieldBlock.isNull(rawIndex)) {
                assertNull(expectedValue);
                continue;
            }
            checkField(fieldBlock, rowField.getType(), rawIndex, expectedValue);
        }
    }

    private static void checkField(Block actualBlock, Type type, int position, Object expectedValue)
    {
        assertNotNull(type, "Type is null");
        assertNotNull(actualBlock, "actualBlock is null");
        assertFalse(actualBlock.isNull(position));
        assertNotNull(expectedValue, "expectedValue is null");

        if (type instanceof ArrayType arrayType) {
            checkArrayValues(arrayType.getObject(actualBlock, position), type, expectedValue);
        }
        else if (type instanceof MapType mapType) {
            checkMapValues(mapType.getObject(actualBlock, position), type, expectedValue);
        }
        else if (type instanceof RowType rowType) {
            checkRowValues(rowType.getObject(actualBlock, position), type, expectedValue);
        }
        else {
            checkPrimitiveValue(type.getObjectValue(SESSION, actualBlock, position), expectedValue);
        }
    }
}
