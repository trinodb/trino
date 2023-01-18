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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static io.trino.parquet.writer.ParquetSchemaConverter.HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING;
import static io.trino.parquet.writer.ParquetSchemaConverter.HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.StructuralTestUtil.arrayType;
import static io.trino.testing.StructuralTestUtil.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetSchemaConverter
{
    @Test
    public void testDecimalTypeLength()
    {
        for (int precision = 1; precision <= MAX_PRECISION; precision++) {
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    ImmutableList.of(createDecimalType(precision)),
                    ImmutableList.of("test"),
                    false,
                    false);
            PrimitiveType primitiveType = schemaConverter.getMessageType().getType(0).asPrimitiveType();
            if (precision <= 9) {
                assertThat(primitiveType.getPrimitiveTypeName())
                        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);
            }
            else if (precision <= MAX_SHORT_PRECISION) {
                assertThat(primitiveType.getPrimitiveTypeName())
                        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
            }
            else {
                assertThat(primitiveType.getPrimitiveTypeName())
                        .isEqualTo(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                assertThat(primitiveType.getTypeLength()).isBetween(9, 16);
                BigInteger bigInteger = new BigInteger("9".repeat(precision));
                assertThat(bigInteger.toByteArray().length).isEqualTo(primitiveType.getTypeLength());
            }
        }
    }

    @Test
    public void testDecimalTypeLengthWithLegacyEncoding()
    {
        for (int precision = 1; precision <= MAX_PRECISION; precision++) {
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    ImmutableList.of(createDecimalType(precision)),
                    ImmutableList.of("test"),
                    HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING,
                    false);
            PrimitiveType primitiveType = schemaConverter.getMessageType().getType(0).asPrimitiveType();
            assertThat(primitiveType.getPrimitiveTypeName())
                    .isEqualTo(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertThat(primitiveType.getTypeLength()).isBetween(1, 16);
            BigInteger bigInteger = new BigInteger("9".repeat(precision));
            assertThat(bigInteger.toByteArray().length).isEqualTo(primitiveType.getTypeLength());
        }
    }

    @Test
    public void testMapKeyRepetitionLevel()
    {
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                ImmutableList.of(mapType(VARCHAR, INTEGER)),
                ImmutableList.of("test"),
                false,
                false);
        GroupType mapType = schemaConverter.getMessageType().getType(0).asGroupType();
        GroupType keyValueValue = mapType.getType(0).asGroupType();
        assertThat(keyValueValue.isRepetition(REPEATED)).isTrue();
        Type keyType = keyValueValue.getType(0).asPrimitiveType();
        assertThat(keyType.isRepetition(REQUIRED)).isTrue();
        PrimitiveType valueType = keyValueValue.getType(1).asPrimitiveType();
        assertThat(valueType.isRepetition(OPTIONAL)).isTrue();

        schemaConverter = new ParquetSchemaConverter(
                ImmutableList.of(mapType(rowType(field("a", VARCHAR), field("b", BIGINT)), INTEGER)),
                ImmutableList.of("test"),
                false,
                false);
        mapType = schemaConverter.getMessageType().getType(0).asGroupType();
        keyValueValue = mapType.getType(0).asGroupType();
        assertThat(keyValueValue.isRepetition(REPEATED)).isTrue();
        keyType = keyValueValue.getType(0).asGroupType();
        assertThat(keyType.isRepetition(REQUIRED)).isTrue();
        assertThat(keyType.asGroupType().getType(0).asPrimitiveType().isRepetition(OPTIONAL)).isTrue();
        assertThat(keyType.asGroupType().getType(1).asPrimitiveType().isRepetition(OPTIONAL)).isTrue();
        valueType = keyValueValue.getType(1).asPrimitiveType();
        assertThat(valueType.isRepetition(OPTIONAL)).isTrue();
    }

    /**
     * This test ensures the parquet writer matches the parquet spec for lists.
     *
     * From https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     *
     * Lists
     * LIST is used to annotate types that should be interpreted as lists.
     * LIST must always annotate a 3-level structure:
     * {@code
     * <list-repetition> group <name> (LIST) {
     *   repeated group list {
     *     <element-repetition> <element-type> element;
     *   }
     * }
     * }
     */
    @Test
    public void testArrayMatchesParquetSpec()
    {
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                ImmutableList.of(arrayType(VARCHAR)),
                ImmutableList.of("tags"),
                false,
                false);

        GroupType outerGroup = schemaConverter.getMessageType().getType(0).asGroupType();
        GroupType innerGroup = outerGroup.getType(0).asGroupType();
        PrimitiveType elementType = innerGroup.getType(0).asPrimitiveType();

        assertThat(outerGroup.getName()).isEqualTo("tags");
        assertThat(outerGroup.getLogicalTypeAnnotation()).isEqualTo(LogicalTypeAnnotation.ListLogicalTypeAnnotation.listType());
        assertThat(outerGroup.getFieldCount()).isEqualTo(1);
        assertThat(outerGroup.getRepetition()).isEqualTo(OPTIONAL);

        assertThat(innerGroup.getName()).isEqualTo("list");
        assertThat(innerGroup.getFieldCount()).isEqualTo(1);
        assertThat(innerGroup.getRepetition()).isEqualTo(REPEATED);

        assertThat(elementType.getName()).isEqualTo("element");
        assertThat(elementType.getPrimitiveTypeName()).isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
        assertThat(elementType.getLogicalTypeAnnotation()).isEqualTo(LogicalTypeAnnotation.stringType());
        assertThat(elementType.getRepetition()).isEqualTo(OPTIONAL);
    }

    @Test
    public void testInt64BackedTimestamps()
    {
        for (int precision = 1; precision <= 9; precision++) {
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    ImmutableList.of(createTimestampType(precision)),
                    ImmutableList.of("test"),
                    false,
                    false);
            PrimitiveType primitiveType = schemaConverter.getMessageType().getType(0).asPrimitiveType();
            assertThat(primitiveType.getPrimitiveTypeName())
                    .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
            if (precision <= 3) {
                assertThat(primitiveType.getLogicalTypeAnnotation())
                        .isEqualTo(timestampType(false, MILLIS));
            }
            else if (precision <= 6) {
                assertThat(primitiveType.getLogicalTypeAnnotation())
                        .isEqualTo(timestampType(false, MICROS));
            }
            else {
                assertThat(primitiveType.getLogicalTypeAnnotation())
                        .isEqualTo(timestampType(false, NANOS));
            }
        }
    }

    @Test
    public void testInt96BackedTimestamps()
    {
        for (int precision = 1; precision <= TIMESTAMP_NANOS.getPrecision(); precision++) {
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    ImmutableList.of(createTimestampType(precision)),
                    ImmutableList.of("test"),
                    false,
                    HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING);
            PrimitiveType primitiveType = schemaConverter.getMessageType().getType(0).asPrimitiveType();
            assertThat(primitiveType.getPrimitiveTypeName())
                    .isEqualTo(PrimitiveType.PrimitiveTypeName.INT96);
            assertThat(primitiveType.getLogicalTypeAnnotation()).isNull();
        }
    }
}
