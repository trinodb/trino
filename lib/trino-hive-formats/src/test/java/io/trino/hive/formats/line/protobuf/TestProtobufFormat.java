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
package io.trino.hive.formats.line.protobuf;

import com.google.protobuf.ByteString;
import io.airlift.units.Duration;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.protobuf.examples.DataRecordProtos.DataRecord;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.SqlVarbinary;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.hive.formats.line.protobuf.examples.DataRecordProtos.DataRecord.EnumType.ENUM1;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class TestProtobufFormat
{
    private static Stream<Arguments> testCases()
    {
        return Stream.of(
                argumentSet("empty fields", emptyFields(), expectedNoValues()),
                argumentSet("all single fields", allSingleFields(), expectedSingleValues()),
                argumentSet("all array fields", allArrayFields(), expectedArrayValues()));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testAllProtobufTypes(DataRecord dataRecord, List<Object> expectedValues)
            throws Exception
    {
        List<Column> columns = List.of(
                new Column("doubleField", DOUBLE, 0),
                new Column("floatField", REAL, 1),
                new Column("int32Field", INTEGER, 2),
                new Column("int64Field", BIGINT, 3),
                new Column("uint32Field", INTEGER, 4),
                new Column("uint64Field", BIGINT, 5),
                new Column("sint32Field", INTEGER, 6),
                new Column("sint64Field", BIGINT, 7),
                new Column("fixed32Field", INTEGER, 8),
                new Column("fixed64Field", BIGINT, 9),
                new Column("sfixed32Field", INTEGER, 10),
                new Column("sfixed64Field", BIGINT, 11),
                new Column("boolField", BOOLEAN, 12),
                new Column("stringField", VARCHAR, 13),
                new Column("enumField", VARCHAR, 14),
                new Column("bytesField", VARBINARY, 15),
                new Column("innerRecord", rowType(
                        field("doubleField", DOUBLE),
                        field("floatField", REAL),
                        field("int32Field", INTEGER),
                        field("int64Field", BIGINT),
                        field("uint32Field", INTEGER),
                        field("uint64Field", BIGINT),
                        field("sint32Field", INTEGER),
                        field("sint64Field", BIGINT),
                        field("fixed32Field", INTEGER),
                        field("fixed64Field", BIGINT),
                        field("sfixed32Field", INTEGER),
                        field("sfixed64Field", BIGINT),
                        field("boolField", BOOLEAN),
                        field("stringField", VARCHAR),
                        field("enumField", VARCHAR),
                        field("bytesField", VARBINARY),
                        field("innerRecord", rowType(
                                field("doubleField", DOUBLE),
                                field("floatField", REAL),
                                field("int32Field", INTEGER),
                                field("int64Field", BIGINT),
                                field("uint32Field", INTEGER),
                                field("uint64Field", BIGINT),
                                field("sint32Field", INTEGER),
                                field("sint64Field", BIGINT),
                                field("fixed32Field", INTEGER),
                                field("fixed64Field", BIGINT),
                                field("sfixed32Field", INTEGER),
                                field("sfixed64Field", BIGINT),
                                field("boolField", BOOLEAN),
                                field("stringField", VARCHAR),
                                field("enumField", VARCHAR),
                                field("bytesField", VARBINARY)))), 16),
                new Column("doubleArrayField", new ArrayType(DOUBLE), 17),
                new Column("floatArrayField", new ArrayType(REAL), 18),
                new Column("int32ArrayField", new ArrayType(INTEGER), 19),
                new Column("int64ArrayField", new ArrayType(BIGINT), 20),
                new Column("uint32ArrayField", new ArrayType(INTEGER), 21),
                new Column("uint64ArrayField", new ArrayType(BIGINT), 22),
                new Column("sint32ArrayField", new ArrayType(INTEGER), 23),
                new Column("sint64ArrayField", new ArrayType(BIGINT), 24),
                new Column("fixed32ArrayField", new ArrayType(INTEGER), 25),
                new Column("fixed64ArrayField", new ArrayType(BIGINT), 26),
                new Column("sfixed32ArrayField", new ArrayType(INTEGER), 27),
                new Column("sfixed64ArrayField", new ArrayType(BIGINT), 28),
                new Column("boolArrayField", new ArrayType(BOOLEAN), 29),
                new Column("stringArrayField", new ArrayType(VARCHAR), 30),
                new Column("enumArrayField", new ArrayType(VARCHAR), 31),
                new Column("bytesArrayField", new ArrayType(VARBINARY), 32),
                new Column("innerRecordArray", new ArrayType(rowType(
                        field("doubleArrayField", new ArrayType(DOUBLE)),
                        field("floatArrayField", new ArrayType(REAL)),
                        field("int32ArrayField", new ArrayType(INTEGER)),
                        field("int64ArrayField", new ArrayType(BIGINT)),
                        field("uint32ArrayField", new ArrayType(INTEGER)),
                        field("uint64ArrayField", new ArrayType(BIGINT)),
                        field("sint32ArrayField", new ArrayType(INTEGER)),
                        field("sint64ArrayField", new ArrayType(BIGINT)),
                        field("fixed32ArrayField", new ArrayType(INTEGER)),
                        field("fixed64ArrayField", new ArrayType(BIGINT)),
                        field("sfixed32ArrayField", new ArrayType(INTEGER)),
                        field("sfixed64ArrayField", new ArrayType(BIGINT)),
                        field("boolArrayField", new ArrayType(BOOLEAN)),
                        field("stringArrayField", new ArrayType(VARCHAR)),
                        field("enumArrayField", new ArrayType(VARCHAR)),
                        field("bytesArrayField", new ArrayType(VARBINARY)),
                        field("innerRecordArray", new ArrayType(rowType(
                                field("doubleArrayField", new ArrayType(DOUBLE)),
                                field("floatArrayField", new ArrayType(REAL)),
                                field("int32ArrayField", new ArrayType(INTEGER)),
                                field("int64ArrayField", new ArrayType(BIGINT)),
                                field("uint32ArrayField", new ArrayType(INTEGER)),
                                field("uint64ArrayField", new ArrayType(BIGINT)),
                                field("sint32ArrayField", new ArrayType(INTEGER)),
                                field("sint64ArrayField", new ArrayType(BIGINT)),
                                field("fixed32ArrayField", new ArrayType(INTEGER)),
                                field("fixed64ArrayField", new ArrayType(BIGINT)),
                                field("sfixed32ArrayField", new ArrayType(INTEGER)),
                                field("sfixed64ArrayField", new ArrayType(BIGINT)),
                                field("boolArrayField", new ArrayType(BOOLEAN)),
                                field("stringArrayField", new ArrayType(VARCHAR)),
                                field("enumArrayField", new ArrayType(VARCHAR)),
                                field("bytesArrayField", new ArrayType(VARBINARY))))))), 33));

        ProtobufDeserializerFactory factory = new ProtobufDeserializerFactory(Paths.get(getClass().getResource("/protobuf/descriptors").toURI()), new Duration(1, HOURS), 1);
        ProtobufDeserializer deserializer = factory.create(columns, Map.of("serialization.class", "io.trino.hive.formats.line.protobuf.examples.DataRecordProtos$DataRecord"));

        LineBuffer lineBuffer = new LineBuffer(128, 1024);
        PageBuilder pageBuilder = new PageBuilder(1, deserializer.getTypes());
        lineBuffer.write(dataRecord.toByteArray());

        deserializer.deserialize(lineBuffer, pageBuilder);

        assertThat(readTrinoValues(columns, pageBuilder.build(), 0)).isEqualTo(expectedValues);
    }

    private static DataRecord allSingleFields()
    {
        return createSingleRecord()
                .setInnerRecord(createSingleRecord().build())
                .build();
    }

    private static DataRecord emptyFields()
    {
        return DataRecord.newBuilder().build();
    }

    private static DataRecord allArrayFields()
    {
        return createArrayRecord()
                .addInnerRecordArray(createArrayRecord().addInnerRecordArray(createArrayRecord()))
                .build();
    }

    private static List<Object> createPrimitiveDefaults()
    {
        List<Object> inner = new ArrayList<>();
        for (int i = 0; i < 17; i++) {
            inner.add(i == 13 ? "theDefault" : null);
        }

        return List.of(
                0d,
                0f,
                0,
                0L,
                0,
                0L,
                0,
                0L,
                0,
                0L,
                0,
                0L,
                false,
                "theDefault",
                "ENUM0",
                new SqlVarbinary(new byte[0]),
                inner);
    }

    private static DataRecord.Builder createSingleRecord()
    {
        return DataRecord.newBuilder()
                .setDoubleField(13.37d)
                .setFloatField(13.37f)
                .setInt32Field(1337)
                .setInt64Field(1337L)
                .setUint32Field(1337)
                .setUint64Field(1337L)
                .setSint32Field(1337)
                .setSint64Field(1337L)
                .setFixed32Field(1337)
                .setFixed64Field(1337L)
                .setSfixed32Field(1337)
                .setSfixed64Field(1337)
                .setBoolField(true)
                .setStringField("foobar")
                .setEnumField(ENUM1)
                .setBytesField(ByteString.copyFrom("hello", UTF_8));
    }

    private static DataRecord.Builder createArrayRecord()
    {
        return DataRecord.newBuilder()
                .addDoubleArrayField(13.37d)
                .addFloatArrayField(13.37f)
                .addInt32ArrayField(1337)
                .addInt64ArrayField(1337L)
                .addUint32ArrayField(1337)
                .addUint64ArrayField(1337L)
                .addSint32ArrayField(1337)
                .addSint64ArrayField(1337L)
                .addFixed32ArrayField(1337)
                .addFixed64ArrayField(1337L)
                .addSfixed32ArrayField(1337)
                .addSfixed64ArrayField(1337)
                .addBoolArrayField(true)
                .addStringArrayField("foobar")
                .addEnumArrayField(ENUM1)
                .addBytesArrayField(ByteString.copyFrom("hello", UTF_8));
    }

    private static List<Object> expectedNoValues()
    {
        List<Object> expected = new ArrayList<>(createPrimitiveDefaults());

        for (int i = 0; i < 17; i++) {
            expected.add(List.of());
        }
        return expected;
    }

    private static List<Object> expectedSingleValues()
    {
        List<Object> dataRecordValues = new ArrayList<>(List.of(
                13.37d,
                13.37f,
                1337,
                1337L,
                1337,
                1337L,
                1337,
                1337L,
                1337,
                1337L,
                1337,
                1337L,
                true,
                "foobar",
                "ENUM1",
                new SqlVarbinary("hello".getBytes(UTF_8))));

        List<Object> expected = new ArrayList<>(dataRecordValues);

        dataRecordValues.add(null);
        expected.add(dataRecordValues);

        for (int i = 0; i < 17; i++) {
            expected.add(List.of());
        }

        return expected;
    }

    private static List<Object> expectedArrayRowValues()
    {
        List<Object> expected = new ArrayList<>();
        expected.add(List.of(13.37d));
        expected.add(List.of(13.37f));
        expected.add(List.of(1337));
        expected.add(List.of(1337L));
        expected.add(List.of(1337));
        expected.add(List.of(1337L));
        expected.add(List.of(1337));
        expected.add(List.of(1337L));
        expected.add(List.of(1337));
        expected.add(List.of(1337L));
        expected.add(List.of(1337));
        expected.add(List.of(1337L));
        expected.add(List.of(true));
        expected.add(List.of("foobar"));
        expected.add(List.of("ENUM1"));
        expected.add(List.of(new SqlVarbinary("hello".getBytes(UTF_8))));

        return expected;
    }

    private static List<Object> expectedArrayValues()
    {
        List<Object> expected = new ArrayList<>(createPrimitiveDefaults());
        expected.addAll(expectedArrayRowValues());

        List<Object> innerRow = expectedArrayRowValues();
        expected.add(List.of(innerRow));
        innerRow.add(List.of(expectedArrayRowValues()));

        return expected;
    }
}
