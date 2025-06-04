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
package io.trino.plugin.deltalake.transactionlog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeTable;
import io.trino.plugin.deltalake.TestingComplexTypeManager;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeColumnType;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeSchemaAsJson;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeStatsAsJson;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestDeltaLakeSchemaSupport
{
    private static final TestingComplexTypeManager typeManager = new TestingComplexTypeManager();

    @Test
    public void testSinglePrimitiveFieldSchema()
    {
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(VARCHAR).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(INTEGER).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(SMALLINT).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(TINYINT).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(REAL).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(DOUBLE).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(BOOLEAN).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(VARBINARY).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(DATE).setNullable(true).build());
        testSinglePrimitiveFieldSchema(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}",
                ColumnMetadata.builder().setName("a").setType(TIMESTAMP_TZ_MILLIS).setNullable(true).build());
    }

    private void testSinglePrimitiveFieldSchema(String json, ColumnMetadata metadata)
    {
        List<ColumnMetadata> schema = DeltaLakeSchemaSupport.getColumnMetadata(json, typeManager, ColumnMappingMode.NONE, List.of()).stream()
                .map(DeltaLakeColumnMetadata::columnMetadata)
                .collect(toImmutableList());
        assertThat(schema).hasSize(1);
        assertThat(schema.get(0)).isEqualTo(metadata);
    }

    // |-- a: integer (nullable = false)
    // |-- b: struct (nullable = true)
    // |    |-- b1: integer (nullable = false)
    // |    |-- b2: struct (nullable = true)
    // |        |-- b21: string (nullable = true)
    // |        |-- b22: boolean (nullable = false)
    // |-- c: array (nullable = true)
    // |    |-- element: integer (containsNull = false)
    // |-- d: array (nullable = true)
    // |    |-- element: struct (containsNull = true)
    // |    |    |-- d1: integer (nullable = false)
    // |-- e: map (nullable = true)
    // |    |-- key: string
    // |    |-- value: struct (valueContainsNull = true)
    // |        |-- e1: date (nullable = true)
    // |        |-- e2: timestamp (nullable = false)
    @Test
    public void testComplexSchema()
            throws IOException, URISyntaxException
    {
        URL expected = getResource("io/trino/plugin/deltalake/transactionlog/schema/complex_schema.json");
        String json = Files.readString(Path.of(expected.toURI()));

        List<ColumnMetadata> schema = DeltaLakeSchemaSupport.getColumnMetadata(json, typeManager, ColumnMappingMode.NONE, List.of()).stream()
                .map(DeltaLakeColumnMetadata::columnMetadata)
                .collect(toImmutableList());
        assertThat(schema).hasSize(5);
        // asserting on the string representations, since they're more readable
        assertThat(schema.get(0).toString()).isEqualTo("ColumnMetadata{name='a', type=integer, nullable}");
        assertThat(schema.get(1).toString()).isEqualTo("ColumnMetadata{name='b', type=row(b1 integer, b2 row(b21 varchar, b22 boolean)), nullable}");
        assertThat(schema.get(2).toString()).isEqualTo("ColumnMetadata{name='c', type=array(integer), nullable}");
        assertThat(schema.get(3).toString()).isEqualTo("ColumnMetadata{name='d', type=array(row(d1 integer)), nullable}");
        assertThat(schema.get(4).toString()).isEqualTo("ColumnMetadata{name='e', type=map(varchar, row(e1 date, e2 timestamp(3) with time zone)), nullable}");
    }

    @Test
    public void testSerializeStatisticsAsJson()
            throws JsonProcessingException
    {
        assertThat(serializeStatsAsJson(
                new DeltaLakeJsonFileStatistics(
                        Optional.of(100L),
                        Optional.of(ImmutableMap.of("c", 42)),
                        Optional.of(ImmutableMap.of("c", 51)),
                        Optional.of(ImmutableMap.of("c", 1L))))).isEqualTo("{\"numRecords\":100,\"minValues\":{\"c\":42},\"maxValues\":{\"c\":51},\"nullCount\":{\"c\":1}}");
    }

    @Test
    public void testSerializeStatisticsWithNullValuesAsJson()
            throws JsonProcessingException
    {
        Map<String, Object> minValues = new HashMap<>();
        Map<String, Object> maxValues = new HashMap<>();

        // Case where the file contains one record and the column `c1` is a null in the record.
        minValues.put("c1", null);
        maxValues.put("c1", null);
        minValues.put("c2", 10);
        maxValues.put("c2", 26);

        assertThat(serializeStatsAsJson(
                new DeltaLakeJsonFileStatistics(
                        Optional.of(1L),
                        Optional.of(minValues),
                        Optional.of(maxValues),
                        Optional.of(ImmutableMap.of("c1", 1L, "c2", 0L))))).isEqualTo("{\"numRecords\":1,\"minValues\":{\"c2\":10},\"maxValues\":{\"c2\":26},\"nullCount\":{\"c1\":1,\"c2\":0}}");
    }

    @Test
    public void testSerializeSchemaAsJson()
            throws Exception
    {
        DeltaLakeColumnHandle arrayColumn = new DeltaLakeColumnHandle(
                "arr",
                new ArrayType(new ArrayType(INTEGER)),
                OptionalInt.empty(),
                "arr",
                new ArrayType(new ArrayType(INTEGER)),
                REGULAR,
                Optional.empty());

        DeltaLakeColumnHandle structColumn = new DeltaLakeColumnHandle(
                "str",
                RowType.from(ImmutableList.of(
                        new RowType.Field(Optional.of("s1"), VarcharType.createUnboundedVarcharType()),
                        new RowType.Field(Optional.of("s2"), RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("i1"), INTEGER),
                                new RowType.Field(Optional.of("d2"), DecimalType.createDecimalType(38, 0))))))),
                OptionalInt.empty(),
                "str",
                RowType.from(ImmutableList.of(
                        new RowType.Field(Optional.of("s1"), VarcharType.createUnboundedVarcharType()),
                        new RowType.Field(Optional.of("s2"), RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("i1"), INTEGER),
                                new RowType.Field(Optional.of("d2"), DecimalType.createDecimalType(38, 0))))))),
                REGULAR,
                Optional.empty());

        TypeOperators typeOperators = new TypeOperators();
        DeltaLakeColumnHandle mapColumn = new DeltaLakeColumnHandle(
                "m",
                new MapType(
                        INTEGER,
                        new MapType(INTEGER, INTEGER, typeOperators),
                        typeOperators),
                OptionalInt.empty(),
                "m",
                new MapType(
                        INTEGER,
                        new MapType(INTEGER, INTEGER, typeOperators),
                        typeOperators),
                REGULAR,
                Optional.empty());

        URL expected = getResource("io/trino/plugin/deltalake/transactionlog/schema/nested_schema.json");
        ObjectMapper objectMapper = new ObjectMapper();

        List<DeltaLakeColumnHandle> columnHandles = ImmutableList.of(arrayColumn, structColumn, mapColumn);
        DeltaLakeTable.Builder deltaTable = DeltaLakeTable.builder();
        for (DeltaLakeColumnHandle column : columnHandles) {
            deltaTable.addColumn(column.columnName(), serializeColumnType(ColumnMappingMode.NONE, new AtomicInteger(), column.baseType()), true, null, ImmutableMap.of());
        }

        String jsonEncoding = serializeSchemaAsJson(deltaTable.build());
        assertThat(objectMapper.readTree(jsonEncoding)).isEqualTo(objectMapper.readTree(expected));
    }

    @Test
    public void testRoundTripComplexSchema()
            throws IOException, URISyntaxException
    {
        URL expected = getResource("io/trino/plugin/deltalake/transactionlog/schema/complex_schema.json");
        String json = Files.readString(Path.of(expected.toURI()));

        List<ColumnMetadata> schema = DeltaLakeSchemaSupport.getColumnMetadata(json, typeManager, ColumnMappingMode.NONE, List.of()).stream()
                .map(DeltaLakeColumnMetadata::columnMetadata)
                .collect(toImmutableList());

        DeltaLakeTable.Builder deltaTable = DeltaLakeTable.builder();
        for (ColumnMetadata column : schema) {
            deltaTable.addColumn(column.getName(), serializeColumnType(ColumnMappingMode.NONE, new AtomicInteger(), column.getType()), true, null, ImmutableMap.of());
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonEncoding = serializeSchemaAsJson(deltaTable.build());
        assertThat(objectMapper.readTree(jsonEncoding)).isEqualTo(objectMapper.readTree(expected));
    }

    @Test
    public void testValidPrimitiveTypes()
    {
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(BIGINT)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(INTEGER)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(SMALLINT)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(TINYINT)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(REAL)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(DOUBLE)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(BOOLEAN)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(VARBINARY)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(DATE)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(VARCHAR)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(DecimalType.createDecimalType(3))).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(TIMESTAMP_TZ_MILLIS)).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(new MapType(TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MILLIS, new TypeOperators()))).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(RowType.anonymous(ImmutableList.of(TIMESTAMP_TZ_MILLIS)))).doesNotThrowAnyException();
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(new ArrayType(TIMESTAMP_TZ_MILLIS))).doesNotThrowAnyException();
    }

    @Test
    public void testValidateTypeFailsOnUnsupportedPrimitiveType()
    {
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(CharType.createCharType(3))).hasMessage("Unsupported type: " + CharType.createCharType(3));
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(TIMESTAMP_MILLIS)).hasMessage("Unsupported type: " + TIMESTAMP_MILLIS);
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(TIMESTAMP_SECONDS)).hasMessage("Unsupported type: " + TIMESTAMP_SECONDS);
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(INTERVAL_DAY_TIME)).hasMessage("Unsupported type: " + INTERVAL_DAY_TIME);
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(INTERVAL_YEAR_MONTH)).hasMessage("Unsupported type: " + INTERVAL_YEAR_MONTH);
    }

    @Test
    public void testTimestampNestedInStructTypeIsNotSupported()
    {
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(new MapType(TIMESTAMP_TZ_SECONDS, TIMESTAMP_TZ_SECONDS, new TypeOperators()))).hasMessage("Unsupported type: timestamp(0) with time zone");
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(RowType.anonymous(ImmutableList.of(TIMESTAMP_TZ_SECONDS)))).hasMessage("Unsupported type: timestamp(0) with time zone");
        assertThatCode(() -> DeltaLakeSchemaSupport.validateType(new ArrayType(TIMESTAMP_TZ_SECONDS))).hasMessage("Unsupported type: timestamp(0) with time zone");
    }
}
