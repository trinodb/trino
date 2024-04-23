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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.cloud.bigquery.StandardSQLTypeName.INT64;
import static com.google.cloud.bigquery.StandardSQLTypeName.STRING;
import static com.google.cloud.bigquery.StandardSQLTypeName.STRUCT;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochSecondsAndFraction;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.type.JsonType.JSON;
import static java.math.BigDecimal.ONE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @deprecated Use {@link BaseBigQueryTypeMapping}.
 */
@Deprecated
public class TestBigQueryType
{
    private static final BigQueryTypeManager TYPE_MANAGER = new BigQueryTypeManager(TESTING_TYPE_MANAGER);

    @Test
    public void testTimeToStringConverter()
    {
        assertThat(BigQueryTypeManager.timeToStringConverter(
                Long.valueOf(303497217825L)))
                .isEqualTo("'00:00:00.303497'");
    }

    @Test
    public void testTimestampToStringConverter()
    {
        assertThat(BigQueryTypeManager.timestampToStringConverter(
                fromEpochSecondsAndFraction(1585658096, 123_456_000_000L, UTC_KEY)))
                .isEqualTo("2020-03-31 12:34:56.123456");
        assertThat(BigQueryTypeManager.timestampToStringConverter(
                fromEpochSecondsAndFraction(1585658096, 123_456_000_000L, TimeZoneKey.getTimeZoneKey("Asia/Kathmandu"))))
                .isEqualTo("2020-03-31 12:34:56.123456");
    }

    @Test
    public void testDateToStringConverter()
    {
        assertThat(BigQueryTypeManager.dateToStringConverter(
                Long.valueOf(18352)))
                .isEqualTo("'2020-03-31'");
    }

    @Test
    public void testStringToStringConverter()
    {
        assertThat(BigQueryTypeManager.stringToStringConverter(
                utf8Slice("test")))
                .isEqualTo("'test'");

        assertThat(BigQueryTypeManager.stringToStringConverter(
                utf8Slice("test's test")))
                .isEqualTo("'test\\'s test'");
    }

    @Test
    public void testNumericToStringConverter()
    {
        assertThat(BigQueryTypeManager.numericToStringConverter(
                encodeScaledValue(ONE, 9)))
                .isEqualTo("1.000000000");
    }

    @Test
    public void testBytesToStringConverter()
    {
        assertThat(BigQueryTypeManager.bytesToStringConverter(
                wrappedBuffer((byte) 1, (byte) 2, (byte) 3, (byte) 4)))
                .isEqualTo("FROM_BASE64('AQIDBA==')");
    }

    @Test
    void testConvertToTrinoType()
    {
        assertColumnType("BOOL", BOOLEAN);
        assertColumnType("INT64", BIGINT);
        assertColumnType("FLOAT64", DOUBLE);
        assertColumnType("NUMERIC", createDecimalType(38, 9));
        assertColumnType("NUMERIC(1)", createDecimalType(1, 0));
        assertColumnType("NUMERIC(10, 5)", createDecimalType(10, 5));
        assertColumnType("NUMERIC(38, 9)", createDecimalType(38, 9));
        assertColumnType("BIGNUMERIC(1)", createDecimalType(1, 0));
        assertColumnType("BIGNUMERIC(10, 5)", createDecimalType(10, 5));
        assertColumnType("BIGNUMERIC(38, 38)", createDecimalType(38, 38));
        assertColumnType("STRING", VARCHAR);
        assertColumnType("STRING(10)", VARCHAR);
        assertColumnType("BYTES", VARBINARY);
        assertColumnType("DATE", DATE);
        assertColumnType("DATETIME", TIMESTAMP_MICROS);
        assertColumnType("TIMESTAMP", TIMESTAMP_TZ_MICROS);
        assertColumnType("GEOGRAPHY", VARCHAR);
        assertColumnType("JSON", JSON);
        assertColumnType("ARRAY<INT64>", new ArrayType(BIGINT));
    }

    @Test
    void testConvertToTrinoTypeStruct()
    {
        String structType = "STRUCT<x INT64, y ARRAY<STRING>>";
        Field structField = Field.of(
                "col",
                STRUCT,
                Field.of("x", INT64),
                Field.newBuilder("y", STRING).setMode(Field.Mode.REPEATED).build());
        TableInfo tableInfo = TableInfo.of(TableId.of("fake", "table"), StandardTableDefinition.of(Schema.of(structField)));

        ColumnMetadata column = TYPE_MANAGER.convertToTrinoType(
                        List.of("col"),
                        List.of(structType),
                        () -> Optional.of(tableInfo)).stream()
                .collect(onlyElement());
        RowType expected = RowType.from(List.of(
                RowType.field("x", BIGINT),
                RowType.field("y", new ArrayType(VARCHAR))));

        // structs are not parsed, but fetched from the table info
        assertThat(column.getType()).isEqualTo(expected);
        // struct without table info is not recognized
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of(structType))).isEmpty();
    }

    @Test
    void testConvertToTrinoTypeUnsupported()
    {
        // unsupported types are ignored, this includes decimals with precision and/or scale out of range
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("NUMERIC(38, 38)"))).isEmpty();
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("BIGNUMERIC"))).isEmpty();
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("BIGNUMERIC(76, 38)"))).isEmpty();
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("TIME"))).isEmpty();
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("RANGE<DATE>"))).isEmpty();
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("RANGE<DATETIME>"))).isEmpty();
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("RANGE<TIMESTAMP>"))).isEmpty();
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("INTERVAL"))).isEmpty();
        assertThat(TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of("invalid-type"))).isEmpty();
    }

    private static void assertColumnType(String typeString, Type expected)
    {
        ColumnMetadata column = TYPE_MANAGER.convertToTrinoType(List.of("col"), List.of(typeString)).stream().collect(onlyElement());
        assertThat(column.getType()).isEqualTo(expected);
    }
}
