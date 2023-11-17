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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public final class AvroDecoderTestUtil
{
    private AvroDecoderTestUtil() {}

    private static void checkPrimitiveValue(Object actual, Object expected)
    {
        if (actual == null || expected == null) {
            assertThat(expected).isNull();
            assertThat(actual).isNull();
        }
        else if (actual instanceof CharSequence) {
            assertThat(expected instanceof CharSequence || expected instanceof GenericEnumSymbol).isTrue();
            assertThat(actual.toString()).isEqualTo(expected.toString());
        }
        else if (actual instanceof SqlVarbinary) {
            if (expected instanceof GenericFixed) {
                assertThat(((SqlVarbinary) actual).getBytes()).isEqualTo(((GenericFixed) expected).bytes());
            }
            else if (expected instanceof ByteBuffer) {
                assertThat(((SqlVarbinary) actual).getBytes()).isEqualTo(((ByteBuffer) expected).array());
            }
            else {
                fail(format("Unexpected value type %s", actual.getClass()));
            }
        }
        else if (isIntegralType(actual) && isIntegralType(expected)) {
            assertThat(((Number) actual).longValue()).isEqualTo(((Number) expected).longValue());
        }
        else if (isRealType(actual) && isRealType(expected)) {
            assertThat(((Number) actual).doubleValue()).isEqualTo(((Number) expected).doubleValue());
        }
        else {
            assertThat(actual).isEqualTo(expected);
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
        assertThat(type)
                .describedAs("Type is null")
                .isNotNull();
        assertThat(type instanceof ArrayType)
                .describedAs("Unexpected type")
                .isTrue();
        assertThat(block)
                .describedAs("Block is null")
                .isNotNull();
        assertThat(value)
                .describedAs("Value is null")
                .isNotNull();

        List<?> list = (List<?>) value;

        assertThat(block.getPositionCount()).isEqualTo(list.size());
        Type elementType = ((ArrayType) type).getElementType();
        for (int index = 0; index < block.getPositionCount(); index++) {
            if (block.isNull(index)) {
                assertThat(list.get(index)).isNull();
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
        assertThat(type)
                .describedAs("Type is null")
                .isNotNull();
        assertThat(type instanceof MapType)
                .describedAs("Unexpected type")
                .isTrue();
        assertThat(((MapType) type).getKeyType() instanceof VarcharType)
                .describedAs("Unexpected key type")
                .isTrue();
        assertThat(sqlMap)
                .describedAs("sqlMap is null")
                .isNotNull();
        assertThat(value)
                .describedAs("Value is null")
                .isNotNull();

        Map<?, ?> expected = (Map<?, ?>) value;

        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        assertThat(sqlMap.getSize()).isEqualTo(expected.size());
        Type valueType = ((MapType) type).getValueType();
        for (int index = 0; index < sqlMap.getSize(); index++) {
            String actualKey = VARCHAR.getSlice(rawKeyBlock, rawOffset + index).toStringUtf8();
            assertThat(expected.containsKey(actualKey))
                    .describedAs("Key not found: %s".formatted(actualKey))
                    .isTrue();
            if (rawValueBlock.isNull(rawOffset + index)) {
                assertThat(expected.get(actualKey)).isNull();
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
        assertThat(type)
                .describedAs("Type is null")
                .isNotNull();
        assertThat(type instanceof RowType)
                .describedAs("Unexpected type")
                .isTrue();
        assertThat(sqlRow)
                .describedAs("sqlRow is null")
                .isNotNull();
        assertThat(value)
                .describedAs("Value is null")
                .isNotNull();

        GenericRecord record = (GenericRecord) value;
        RowType rowType = (RowType) type;
        assertThat(record.getSchema().getFields().size())
                .describedAs("Avro field size mismatch")
                .isEqualTo(rowType.getFields().size());
        assertThat(sqlRow.getFieldCount())
                .describedAs("Trino type field size mismatch")
                .isEqualTo(rowType.getFields().size());
        int rawIndex = sqlRow.getRawIndex();
        for (int fieldIndex = 0; fieldIndex < rowType.getFields().size(); fieldIndex++) {
            RowType.Field rowField = rowType.getFields().get(fieldIndex);
            Object expectedValue = record.get(rowField.getName().orElseThrow());
            Block fieldBlock = sqlRow.getRawFieldBlock(fieldIndex);
            if (fieldBlock.isNull(rawIndex)) {
                assertThat(expectedValue).isNull();
                continue;
            }
            checkField(fieldBlock, rowField.getType(), rawIndex, expectedValue);
        }
    }

    private static void checkField(Block actualBlock, Type type, int position, Object expectedValue)
    {
        assertThat(type)
                .describedAs("Type is null")
                .isNotNull();
        assertThat(actualBlock)
                .describedAs("actualBlock is null")
                .isNotNull();
        assertThat(actualBlock.isNull(position)).isFalse();
        assertThat(expectedValue)
                .describedAs("expectedValue is null")
                .isNotNull();

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
