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
package io.trino.plugin.paimon;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeOperators;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Consumer;

import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TrinoRowTest
{
    private static final Type JSON_TYPE = TESTING_TYPE_MANAGER.getType(new TypeDescriptor(JSON));

    @Test
    void test()
    {
        Page singlePage = new Page(
                1,
                writeNativeValue(BOOLEAN, null),
                writeNativeValue(BOOLEAN, false),
                writeNativeValue(TINYINT, 22L),
                writeNativeValue(SMALLINT, 356L),
                writeNativeValue(INTEGER, 4L),
                writeNativeValue(BIGINT, 23567222L),
                writeNativeValue(REAL, (long) Float.floatToIntBits(1213.33f)),
                writeNativeValue(DOUBLE, 121.3d),
                writeNativeValue(VARCHAR, Slices.wrappedBuffer("rfyu".getBytes(StandardCharsets.UTF_8))),
                writeNativeValue(DecimalType.createDecimalType(2, 2),
                        encodeShortScaledValue(BigDecimal.valueOf(0.21), 2)),
                writeNativeValue(DecimalType.createDecimalType(38, 2),
                        encodeScaledValue(BigDecimal.valueOf(65782123123.01), 2)),
                writeNativeValue(DecimalType.createDecimalType(10, 1),
                        encodeShortScaledValue(BigDecimal.valueOf(62123123.5), 1)),
                writeNativeValue(TIMESTAMP_MICROS,
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")).getMillisecond()
                                * MICROSECONDS_PER_MILLISECOND),
                writeNativeValue(VARBINARY, Slices.wrappedBuffer("varbinary_v".getBytes(StandardCharsets.UTF_8))),
                writeNativeValue(JSON_TYPE, jsonParse(Slices.utf8Slice("[1,\"two\",true]"))));
        List<Type> types = List.of(
                BOOLEAN,
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                VARCHAR,
                DecimalType.createDecimalType(2, 2),
                DecimalType.createDecimalType(38, 2),
                DecimalType.createDecimalType(10, 1),
                TIMESTAMP_MICROS,
                VARBINARY,
                JSON_TYPE);
        PaimonRow trinoRow = new PaimonRow(singlePage, RowKind.INSERT, types, logicalTypes(types));

        assertThat(trinoRow.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(trinoRow.isNullAt(0)).isEqualTo(true);
        assertThat(trinoRow.getBoolean(1)).isEqualTo(false);
        assertThat(trinoRow.getByte(2)).isEqualTo((byte) 22);
        assertThat(trinoRow.getShort(3)).isEqualTo((short) 356);
        assertThat(trinoRow.getInt(4)).isEqualTo(4);
        assertThat(trinoRow.getLong(5)).isEqualTo(23567222L);
        assertThat(trinoRow.getFloat(6)).isEqualTo(1213.33f);
        assertThat(trinoRow.getDouble(7)).isEqualTo(121.3d);
        assertThat(trinoRow.getString(8)).isEqualTo(BinaryString.fromString("rfyu"));
        assertThat(trinoRow.getDecimal(9, 2, 2)).isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(0.21), 2, 2));
        assertThat(trinoRow.getDecimal(10, 38, 2))
                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(65782123123.01), 38, 2));
        assertThat(trinoRow.getDecimal(11, 10, 1))
                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(62123123.5), 10, 1));
        assertThat(trinoRow.getTimestamp(12, 6))
                .isEqualTo(Timestamp.fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")));
        assertThat(trinoRow.getBinary(13)).isEqualTo("varbinary_v".getBytes(StandardCharsets.UTF_8));
        assertThat(trinoRow.getBlob(13).toData()).isEqualTo("varbinary_v".getBytes(StandardCharsets.UTF_8));
        assertThat(trinoRow.getVariant(14).toJson()).isEqualTo("[1,\"two\",true]");
    }

    @Test
    void testConstructorRejectsNullPageAndRowKind()
    {
        assertThatThrownBy(() -> new PaimonRow(null, RowKind.INSERT, List.of(INTEGER), List.of(DataTypes.INT())))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("singlePage is null");

        assertThatThrownBy(() -> new PaimonRow(
                new Page(1, writeNativeValue(INTEGER, 1L)),
                null,
                List.of(INTEGER),
                List.of(DataTypes.INT())))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rowKind is null");
    }

    @Test
    void testReadsSelectedPositionFromMultiPositionPage()
    {
        BlockBuilder idBuilder = INTEGER.createFixedSizeBlockBuilder(2);
        writeNativeValue(INTEGER, idBuilder, 11L);
        writeNativeValue(INTEGER, idBuilder, 22L);
        BlockBuilder nameBuilder = VARCHAR.createBlockBuilder(null, 2);
        writeNativeValue(VARCHAR, nameBuilder, Slices.utf8Slice("first"));
        writeNativeValue(VARCHAR, nameBuilder, Slices.utf8Slice("second"));
        Page page = new Page(2, idBuilder.build(), nameBuilder.build());

        PaimonRow trinoRow = new PaimonRow(
                page,
                1,
                RowKind.INSERT,
                List.of(INTEGER, VARCHAR),
                logicalTypes(List.of(INTEGER, VARCHAR)));

        assertThat(trinoRow.getInt(0)).isEqualTo(22);
        assertThat(trinoRow.getString(1)).isEqualTo(BinaryString.fromString("second"));
    }

    @Test
    void testTimeAndHighPrecisionTimestampConversions()
    {
        LongTimestamp timestamp = new LongTimestamp(1_695_645_403_123_456L, 789_000);
        LongTimestampWithTimeZone timestampWithTimeZone = fromEpochMillisAndFraction(
                1_695_645_403_123L,
                456_000_000,
                UTC_KEY);
        Page singlePage = new Page(
                1,
                writeNativeValue(TIME_MILLIS, 12_345L * PICOSECONDS_PER_MILLISECOND),
                writeNativeValue(TIMESTAMP_NANOS, timestamp),
                writeNativeValue(TIMESTAMP_TZ_MICROS, timestampWithTimeZone));
        List<Type> types = List.of(TIME_MILLIS, TIMESTAMP_NANOS, TIMESTAMP_TZ_MICROS);
        PaimonRow trinoRow = new PaimonRow(singlePage, RowKind.INSERT, types, logicalTypes(types));

        assertThat(trinoRow.getInt(0)).isEqualTo(12_345);
        assertThat(trinoRow.getTimestamp(1, 9)).isEqualTo(Timestamp.fromEpochMillis(1_695_645_403_123L, 456_789));
        assertThat(trinoRow.getTimestamp(2, 6)).isEqualTo(Timestamp.fromEpochMillis(1_695_645_403_123L, 456_000));
    }

    @Test
    void testNegativeHighPrecisionTimestampConversions()
    {
        LongTimestamp timestamp = new LongTimestamp(-1_234L, 567_000);
        LongTimestampWithTimeZone timestampWithTimeZone = fromEpochMillisAndFraction(-2L, 766_000_000, UTC_KEY);
        Page singlePage = new Page(
                1,
                writeNativeValue(TIMESTAMP_NANOS, timestamp),
                writeNativeValue(TIMESTAMP_TZ_MICROS, timestampWithTimeZone));
        List<Type> types = List.of(TIMESTAMP_NANOS, TIMESTAMP_TZ_MICROS);
        PaimonRow trinoRow = new PaimonRow(singlePage, RowKind.INSERT, types, logicalTypes(types));

        assertThat(trinoRow.getTimestamp(0, 9)).isEqualTo(Timestamp.fromEpochMillis(-2L, 766_567));
        assertThat(trinoRow.getTimestamp(1, 6)).isEqualTo(Timestamp.fromEpochMillis(-2L, 766_000));
    }

    @Test
    void testVariantRequiresJsonTypeMetadata()
    {
        Page singlePage = new Page(1, writeNativeValue(VARCHAR, Slices.utf8Slice("{\"a\":1}")));
        PaimonRow trinoRow = new PaimonRow(
                singlePage,
                RowKind.INSERT,
                List.of(VARCHAR),
                List.of(DataTypes.VARIANT()));

        assertThatThrownBy(() -> trinoRow.getVariant(0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon VARIANT requires Trino JSON type metadata");
    }

    @Test
    public void testBinaryConversionPadsFixedLengthValues()
    {
        Page singlePage = new Page(1, writeNativeValue(VARBINARY, Slices.utf8Slice("ab")));
        PaimonRow trinoRow = new PaimonRow(
                singlePage,
                RowKind.INSERT,
                List.of(VARBINARY),
                List.of(DataTypes.BINARY(4)));

        assertThat(trinoRow.getBinary(0)).containsExactly((byte) 'a', (byte) 'b', (byte) 0, (byte) 0);
    }

    @Test
    public void testBinaryConversionRejectsFixedLengthTruncation()
    {
        Page singlePage = new Page(1, writeNativeValue(VARBINARY, Slices.utf8Slice("abcd")));
        PaimonRow trinoRow = new PaimonRow(
                singlePage,
                RowKind.INSERT,
                List.of(VARBINARY),
                List.of(DataTypes.BINARY(3)));

        assertThatThrownBy(() -> trinoRow.getBinary(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot write 4 bytes to Paimon BINARY(3); value would be truncated");
    }

    @Test
    public void testBinaryConversionRejectsVariableLengthTruncation()
    {
        Page singlePage = new Page(1, writeNativeValue(VARBINARY, Slices.utf8Slice("abcd")));
        PaimonRow trinoRow = new PaimonRow(
                singlePage,
                RowKind.INSERT,
                List.of(VARBINARY),
                List.of(DataTypes.VARBINARY(3)));

        assertThatThrownBy(() -> trinoRow.getBinary(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot write 4 bytes to Paimon VARBINARY(3); value would be truncated");
    }

    @Test
    public void testNestedBinaryConversionRejectsTruncation()
    {
        ArrayType arrayType = new ArrayType(VARBINARY);
        Block array = buildArrayValue(
                arrayType,
                1,
                elementBuilder -> writeNativeValue(VARBINARY, elementBuilder, Slices.utf8Slice("abcd")));
        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(arrayType, array)),
                RowKind.INSERT,
                List.of(arrayType),
                List.of(DataTypes.ARRAY(DataTypes.VARBINARY(3))));

        assertThatThrownBy(() -> trinoRow.getArray(0).getBinary(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot write 4 bytes to Paimon VARBINARY(3); value would be truncated");
    }

    @Test
    public void testNestedMapBinaryConversionRejectsTruncation()
    {
        MapType mapType = new MapType(INTEGER, VARBINARY, new TypeOperators());
        SqlMap map = buildMapValue(mapType, 1, (keyBuilder, valueBuilder) -> {
            writeNativeValue(INTEGER, keyBuilder, 1L);
            writeNativeValue(VARBINARY, valueBuilder, Slices.utf8Slice("abcd"));
        });
        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(mapType, map)),
                RowKind.INSERT,
                List.of(mapType),
                List.of(DataTypes.MAP(DataTypes.INT(), DataTypes.VARBINARY(3))));

        assertThatThrownBy(() -> trinoRow.getMap(0).valueArray().getBinary(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot write 4 bytes to Paimon VARBINARY(3); value would be truncated");
    }

    @Test
    public void testNestedRowBinaryConversionRejectsTruncation()
    {
        RowType rowType = RowType.anonymous(List.of(VARBINARY));
        SqlRow row = buildRowValue(
                rowType,
                fieldBuilders -> writeNativeValue(VARBINARY, fieldBuilders.get(0), Slices.utf8Slice("abcd")));
        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(rowType, row)),
                RowKind.INSERT,
                List.of(rowType),
                List.of(DataTypes.ROW(DataTypes.FIELD(0, "value", DataTypes.VARBINARY(3)))));

        assertThatThrownBy(() -> trinoRow.getRow(0, 1).getBinary(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot write 4 bytes to Paimon VARBINARY(3); value would be truncated");
    }

    @Test
    void testInvalidVariantJsonKeepsParseFailureContext()
    {
        Page singlePage = new Page(1, writeNativeValue(JSON_TYPE, Slices.utf8Slice("{broken")));
        PaimonRow trinoRow = new PaimonRow(
                singlePage,
                RowKind.INSERT,
                List.of(JSON_TYPE),
                List.of(DataTypes.VARIANT()));

        assertThatThrownBy(() -> trinoRow.getVariant(0))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to parse Variant from JSON")
                .rootCause()
                .isInstanceOf(IOException.class);
    }

    @Test
    void testLongDecimalWithUnsignedLowBits()
    {
        DecimalType type = DecimalType.createDecimalType(38, 0);
        BigDecimal value = new BigDecimal("18446744073709551615");
        Page singlePage = new Page(1, writeNativeValue(type, encodeScaledValue(value, type.getScale())));
        PaimonRow trinoRow = new PaimonRow(singlePage, RowKind.INSERT, List.of(type), logicalTypes(List.of(type)));

        assertThat(trinoRow.getDecimal(0, type.getPrecision(), type.getScale()))
                .isEqualTo(Decimal.fromBigDecimal(value, type.getPrecision(), type.getScale()));
    }

    @Test
    void testNestedComplexTypeConversionsUseElementTypes()
    {
        ArrayType timestampArrayType = new ArrayType(TIMESTAMP_NANOS);
        MapType timestampMapType = new MapType(INTEGER, TIMESTAMP_TZ_MICROS, new TypeOperators());
        DecimalType longDecimalType = DecimalType.createDecimalType(38, 0);
        RowType rowType = RowType.anonymous(List.of(TIME_MILLIS, longDecimalType));

        LongTimestamp timestamp = new LongTimestamp(1_695_645_403_123_456L, 789_000);
        LongTimestampWithTimeZone timestampWithTimeZone = fromEpochMillisAndFraction(
                1_695_645_403_123L,
                456_000_000,
                UTC_KEY);
        BigDecimal decimalValue = new BigDecimal("18446744073709551615");

        Block timestampArray = buildArrayValue(
                timestampArrayType,
                1,
                elementBuilder -> writeNativeValue(TIMESTAMP_NANOS, elementBuilder, timestamp));
        SqlMap timestampMap = buildMapValue(timestampMapType, 1, (keyBuilder, valueBuilder) -> {
            writeNativeValue(INTEGER, keyBuilder, 7L);
            writeNativeValue(TIMESTAMP_TZ_MICROS, valueBuilder, timestampWithTimeZone);
        });
        SqlRow row = buildRowValue(rowType, fieldBuilders -> {
            writeNativeValue(TIME_MILLIS, fieldBuilders.get(0), 12_345L * PICOSECONDS_PER_MILLISECOND);
            writeNativeValue(
                    longDecimalType,
                    fieldBuilders.get(1),
                    encodeScaledValue(decimalValue, longDecimalType.getScale()));
        });

        List<Type> types = List.of(timestampArrayType, timestampMapType, rowType);
        PaimonRow trinoRow = new PaimonRow(new Page(
                1,
                writeNativeValue(timestampArrayType, timestampArray),
                writeNativeValue(timestampMapType, timestampMap),
                writeNativeValue(rowType, row)),
                RowKind.INSERT,
                types,
                logicalTypes(types));

        InternalArray array = trinoRow.getArray(0);
        assertThat(array.size()).isEqualTo(1);
        assertThat(array.getTimestamp(0, 9)).isEqualTo(Timestamp.fromEpochMillis(1_695_645_403_123L, 456_789));

        InternalMap map = trinoRow.getMap(1);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.keyArray().getInt(0)).isEqualTo(7);
        assertThat(map.valueArray().getTimestamp(0, 6))
                .isEqualTo(Timestamp.fromEpochMillis(1_695_645_403_123L, 456_000));

        InternalRow nestedRow = trinoRow.getRow(2, 2);
        assertThat(nestedRow.getInt(0)).isEqualTo(12_345);
        assertThat(nestedRow.getDecimal(1, longDecimalType.getPrecision(), longDecimalType.getScale()))
                .isEqualTo(Decimal.fromBigDecimal(
                        decimalValue,
                        longDecimalType.getPrecision(),
                        longDecimalType.getScale()));
    }

    @Test
    void testRowConversionValidatesRequestedFieldCount()
    {
        RowType rowType = RowType.anonymous(List.of(INTEGER, VARCHAR));
        SqlRow row = buildRowValue(rowType, fieldBuilders -> {
            writeNativeValue(INTEGER, fieldBuilders.get(0), 7L);
            writeNativeValue(VARCHAR, fieldBuilders.get(1), Slices.utf8Slice("seven"));
        });
        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(rowType, row)),
                RowKind.INSERT,
                List.of(rowType),
                List.of(DataTypes.ROW(
                        DataTypes.FIELD(0, "f0", DataTypes.INT()),
                        DataTypes.FIELD(1, "f1", DataTypes.STRING()))));

        assertThatThrownBy(() -> trinoRow.getRow(0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon ROW field count mismatch: expected 1, got 2");
    }

    @Test
    void testNestedRowConversionValidatesRequestedFieldCount()
    {
        RowType innerRowType = RowType.anonymous(List.of(INTEGER, VARCHAR));
        RowType outerRowType = RowType.anonymous(List.of(innerRowType));
        SqlRow innerRow = buildRowValue(innerRowType, fieldBuilders -> {
            writeNativeValue(INTEGER, fieldBuilders.get(0), 7L);
            writeNativeValue(VARCHAR, fieldBuilders.get(1), Slices.utf8Slice("seven"));
        });
        SqlRow outerRow = buildRowValue(
                outerRowType,
                fieldBuilders -> writeNativeValue(innerRowType, fieldBuilders.get(0), innerRow));
        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(outerRowType, outerRow)),
                RowKind.INSERT,
                List.of(outerRowType),
                List.of(DataTypes.ROW(DataTypes.FIELD(0, "nested", DataTypes.ROW(
                        DataTypes.FIELD(0, "f0", DataTypes.INT()),
                        DataTypes.FIELD(1, "f1", DataTypes.STRING()))))));

        InternalRow outer = trinoRow.getRow(0, 1);

        assertThatThrownBy(() -> outer.getRow(0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon ROW field count mismatch: expected 1, got 2");
    }

    @Test
    void testMultisetConversionUsesElementAndCountTypes()
    {
        MapType multisetType = new MapType(VARCHAR, INTEGER, new TypeOperators());
        SqlMap multiset = buildMapValue(multisetType, 2, (keyBuilder, valueBuilder) -> {
            writeNativeValue(VARCHAR, keyBuilder, Slices.utf8Slice("red"));
            writeNativeValue(INTEGER, valueBuilder, 2L);
            writeNativeValue(VARCHAR, keyBuilder, Slices.utf8Slice("blue"));
            writeNativeValue(INTEGER, valueBuilder, 1L);
        });

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(multisetType, multiset)),
                RowKind.INSERT,
                List.of(multisetType),
                List.of(DataTypes.MULTISET(DataTypes.STRING())));

        InternalMap paimonMultiset = trinoRow.getMap(0);
        assertThat(paimonMultiset.size()).isEqualTo(2);
        assertThat(paimonMultiset.keyArray().getString(0)).isEqualTo(BinaryString.fromString("red"));
        assertThat(paimonMultiset.valueArray().getInt(0)).isEqualTo(2);
        assertThat(paimonMultiset.keyArray().getString(1)).isEqualTo(BinaryString.fromString("blue"));
        assertThat(paimonMultiset.valueArray().getInt(1)).isEqualTo(1);
    }

    @Test
    void testNestedMultisetConversionUsesElementAndCountTypes()
    {
        MapType multisetType = new MapType(VARCHAR, INTEGER, new TypeOperators());
        RowType rowType = RowType.anonymous(List.of(multisetType));
        SqlMap multiset = buildMapValue(multisetType, 1, (keyBuilder, valueBuilder) -> {
            writeNativeValue(VARCHAR, keyBuilder, Slices.utf8Slice("green"));
            writeNativeValue(INTEGER, valueBuilder, 3L);
        });
        SqlRow row = buildRowValue(
                rowType,
                fieldBuilders -> writeNativeValue(multisetType, fieldBuilders.get(0), multiset));

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(rowType, row)),
                RowKind.INSERT,
                List.of(rowType),
                List.of(DataTypes.ROW(DataTypes.FIELD(
                        0,
                        "tags",
                        DataTypes.MULTISET(DataTypes.STRING())))));

        InternalMap paimonMultiset = trinoRow.getRow(0, 1).getMap(0);
        assertThat(paimonMultiset.size()).isEqualTo(1);
        assertThat(paimonMultiset.keyArray().getString(0)).isEqualTo(BinaryString.fromString("green"));
        assertThat(paimonMultiset.valueArray().getInt(0)).isEqualTo(3);
    }

    @Test
    void testMultisetConversionRejectsNullCounts()
    {
        MapType multisetType = new MapType(VARCHAR, INTEGER, new TypeOperators());
        SqlMap multiset = buildMapValue(multisetType, 1, (keyBuilder, valueBuilder) -> {
            writeNativeValue(VARCHAR, keyBuilder, Slices.utf8Slice("red"));
            valueBuilder.appendNull();
        });

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(multisetType, multiset)),
                RowKind.INSERT,
                List.of(multisetType),
                List.of(DataTypes.MULTISET(DataTypes.STRING())));

        assertThatThrownBy(() -> trinoRow.getMap(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon MULTISET does not allow null counts");
    }

    @Test
    void testMultisetConversionRejectsNonPositiveCounts()
    {
        assertMultisetCountRejected(0);
        assertMultisetCountRejected(-1);
    }

    @Test
    void testMultisetConversionRequiresIntegerCountType()
    {
        MapType multisetType = new MapType(VARCHAR, BIGINT, new TypeOperators());
        SqlMap multiset = buildMapValue(multisetType, 1, (keyBuilder, valueBuilder) -> {
            writeNativeValue(VARCHAR, keyBuilder, Slices.utf8Slice("red"));
            writeNativeValue(BIGINT, valueBuilder, 2L);
        });

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(multisetType, multiset)),
                RowKind.INSERT,
                List.of(multisetType),
                List.of(DataTypes.MULTISET(DataTypes.STRING())));

        assertThatThrownBy(() -> trinoRow.getMap(0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon MULTISET requires Trino integer count type metadata");
    }

    private static void assertMultisetCountRejected(int count)
    {
        MapType multisetType = new MapType(VARCHAR, INTEGER, new TypeOperators());
        SqlMap multiset = buildMapValue(multisetType, 1, (keyBuilder, valueBuilder) -> {
            writeNativeValue(VARCHAR, keyBuilder, Slices.utf8Slice("red"));
            writeNativeValue(INTEGER, valueBuilder, (long) count);
        });

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(multisetType, multiset)),
                RowKind.INSERT,
                List.of(multisetType),
                List.of(DataTypes.MULTISET(DataTypes.STRING())));

        assertThatThrownBy(() -> trinoRow.getMap(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon MULTISET count must be positive: " + count);
    }

    @Test
    void testVectorConversionUsesArrayBlock()
    {
        ArrayType vectorType = new ArrayType(REAL);
        Block vector = buildArrayValue(vectorType, 3, elementBuilder -> {
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(1.0f));
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(2.5f));
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(3.75f));
        });

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(vectorType, vector)),
                RowKind.INSERT,
                List.of(vectorType),
                List.of(DataTypes.VECTOR(3, DataTypes.FLOAT())));

        InternalVector paimonVector = trinoRow.getVector(0);
        assertThat(paimonVector.size()).isEqualTo(3);
        assertThat(paimonVector.toFloatArray()).isEqualTo(new float[] {1.0f, 2.5f, 3.75f});
    }

    @Test
    void testVectorConversionSupportsAllPaimonPrimitiveElementTypes()
    {
        assertVectorConversion(BOOLEAN, DataTypes.BOOLEAN(), elementBuilder -> {
            writeNativeValue(BOOLEAN, elementBuilder, true);
            writeNativeValue(BOOLEAN, elementBuilder, false);
            writeNativeValue(BOOLEAN, elementBuilder, true);
        }, paimonVector -> assertThat(paimonVector.toBooleanArray()).containsExactly(true, false, true));

        assertVectorConversion(TINYINT, DataTypes.TINYINT(), elementBuilder -> {
            writeNativeValue(TINYINT, elementBuilder, 1L);
            writeNativeValue(TINYINT, elementBuilder, 2L);
            writeNativeValue(TINYINT, elementBuilder, 3L);
        }, paimonVector -> assertThat(paimonVector.toByteArray()).containsExactly((byte) 1, (byte) 2, (byte) 3));

        assertVectorConversion(SMALLINT, DataTypes.SMALLINT(), elementBuilder -> {
            writeNativeValue(SMALLINT, elementBuilder, 10L);
            writeNativeValue(SMALLINT, elementBuilder, 20L);
            writeNativeValue(SMALLINT, elementBuilder, 30L);
        }, paimonVector -> assertThat(paimonVector.toShortArray()).containsExactly((short) 10, (short) 20, (short) 30));

        assertVectorConversion(INTEGER, DataTypes.INT(), elementBuilder -> {
            writeNativeValue(INTEGER, elementBuilder, 100L);
            writeNativeValue(INTEGER, elementBuilder, 200L);
            writeNativeValue(INTEGER, elementBuilder, 300L);
        }, paimonVector -> assertThat(paimonVector.toIntArray()).containsExactly(100, 200, 300));

        assertVectorConversion(BIGINT, DataTypes.BIGINT(), elementBuilder -> {
            writeNativeValue(BIGINT, elementBuilder, 1_000L);
            writeNativeValue(BIGINT, elementBuilder, 2_000L);
            writeNativeValue(BIGINT, elementBuilder, 3_000L);
        }, paimonVector -> assertThat(paimonVector.toLongArray()).containsExactly(1_000L, 2_000L, 3_000L));

        assertVectorConversion(REAL, DataTypes.FLOAT(), elementBuilder -> {
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(1.0f));
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(2.5f));
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(3.75f));
        }, paimonVector -> assertThat(paimonVector.toFloatArray()).containsExactly(1.0f, 2.5f, 3.75f));

        assertVectorConversion(DOUBLE, DataTypes.DOUBLE(), elementBuilder -> {
            writeNativeValue(DOUBLE, elementBuilder, 1.0d);
            writeNativeValue(DOUBLE, elementBuilder, 2.5d);
            writeNativeValue(DOUBLE, elementBuilder, 3.75d);
        }, paimonVector -> assertThat(paimonVector.toDoubleArray()).containsExactly(1.0d, 2.5d, 3.75d));
    }

    @Test
    void testVectorConversionValidatesPaimonLogicalLength()
    {
        ArrayType vectorType = new ArrayType(REAL);
        Block vector = buildArrayValue(vectorType, 2, elementBuilder -> {
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(1.0f));
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(2.5f));
        });

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(vectorType, vector)),
                RowKind.INSERT,
                List.of(vectorType),
                List.of(DataTypes.VECTOR(3, DataTypes.FLOAT())));

        assertThatThrownBy(() -> trinoRow.getVector(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon VECTOR length mismatch: expected 3, got 2");
    }

    @Test
    void testVectorConversionRejectsNullElements()
    {
        ArrayType vectorType = new ArrayType(REAL);
        Block vector = buildArrayValue(vectorType, 3, elementBuilder -> {
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(1.0f));
            elementBuilder.appendNull();
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(3.75f));
        });

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(vectorType, vector)),
                RowKind.INSERT,
                List.of(vectorType),
                List.of(DataTypes.VECTOR(3, DataTypes.FLOAT())));

        assertThatThrownBy(() -> trinoRow.getVector(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon VECTOR does not allow null elements");
    }

    @Test
    void testNestedVectorConversionValidatesPaimonLogicalLength()
    {
        ArrayType vectorType = new ArrayType(REAL);
        RowType rowType = RowType.anonymous(List.of(vectorType));
        Block vector = buildArrayValue(vectorType, 2, elementBuilder -> {
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(1.0f));
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(2.5f));
        });
        SqlRow row = buildRowValue(rowType, fieldBuilders -> writeNativeValue(vectorType, fieldBuilders.get(0), vector));

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(rowType, row)),
                RowKind.INSERT,
                List.of(rowType),
                List.of(new org.apache.paimon.types.RowType(List.of(
                        new DataField(0, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))))));

        InternalRow nestedRow = trinoRow.getRow(0, 1);
        assertThatThrownBy(() -> nestedRow.getVector(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon VECTOR length mismatch: expected 3, got 2");
    }

    @Test
    void testArrayOfVectorConversionValidatesPaimonLogicalLength()
    {
        ArrayType vectorType = new ArrayType(REAL);
        ArrayType arrayOfVectorType = new ArrayType(vectorType);
        Block shortVector = buildArrayValue(vectorType, 2, elementBuilder -> {
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(1.0f));
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(2.5f));
        });
        Block arrayOfVector = buildArrayValue(
                arrayOfVectorType,
                1,
                elementBuilder -> writeNativeValue(vectorType, elementBuilder, shortVector));

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(arrayOfVectorType, arrayOfVector)),
                RowKind.INSERT,
                List.of(arrayOfVectorType),
                List.of(DataTypes.ARRAY(DataTypes.VECTOR(3, DataTypes.FLOAT()))));

        InternalArray array = trinoRow.getArray(0);
        assertThatThrownBy(() -> array.getVector(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon VECTOR length mismatch: expected 3, got 2");
    }

    @Test
    void testMapValueVectorConversionValidatesPaimonLogicalLength()
    {
        ArrayType vectorType = new ArrayType(REAL);
        MapType mapType = new MapType(INTEGER, vectorType, new TypeOperators());
        Block shortVector = buildArrayValue(vectorType, 2, elementBuilder -> {
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(1.0f));
            writeNativeValue(REAL, elementBuilder, (long) Float.floatToIntBits(2.5f));
        });
        SqlMap map = buildMapValue(mapType, 1, (keyBuilder, valueBuilder) -> {
            writeNativeValue(INTEGER, keyBuilder, 7L);
            writeNativeValue(vectorType, valueBuilder, shortVector);
        });

        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(mapType, map)),
                RowKind.INSERT,
                List.of(mapType),
                List.of(DataTypes.MAP(DataTypes.INT(), DataTypes.VECTOR(3, DataTypes.FLOAT()))));

        InternalMap paimonMap = trinoRow.getMap(0);
        assertThatThrownBy(() -> paimonMap.valueArray().getVector(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon VECTOR length mismatch: expected 3, got 2");
    }

    private static List<DataType> logicalTypes(List<Type> types)
    {
        return types.stream()
                .map(PaimonTypeUtils::toPaimonType)
                .toList();
    }

    private static void assertVectorConversion(
            Type elementType,
            DataType elementLogicalType,
            Consumer<BlockBuilder> writeElements,
            Consumer<InternalVector> assertVector)
    {
        ArrayType vectorType = new ArrayType(elementType);
        Block vector = buildArrayValue(vectorType, 3, writeElements::accept);
        PaimonRow trinoRow = new PaimonRow(
                new Page(1, writeNativeValue(vectorType, vector)),
                RowKind.INSERT,
                List.of(vectorType),
                List.of(DataTypes.VECTOR(3, elementLogicalType)));

        InternalVector paimonVector = trinoRow.getVector(0);
        assertThat(paimonVector.size()).isEqualTo(3);
        assertVector.accept(paimonVector);
    }
}
