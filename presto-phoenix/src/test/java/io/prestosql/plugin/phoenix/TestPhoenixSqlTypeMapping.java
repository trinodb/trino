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
package io.prestosql.plugin.phoenix;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.datatype.CreateAndInsertDataSetup;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.BaseEncoding.base16;
import static io.prestosql.plugin.phoenix.PhoenixQueryRunner.createPhoenixQueryRunner;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.testing.datatype.DataType.bigintDataType;
import static io.prestosql.testing.datatype.DataType.booleanDataType;
import static io.prestosql.testing.datatype.DataType.dataType;
import static io.prestosql.testing.datatype.DataType.dateDataType;
import static io.prestosql.testing.datatype.DataType.doubleDataType;
import static io.prestosql.testing.datatype.DataType.integerDataType;
import static io.prestosql.testing.datatype.DataType.realDataType;
import static io.prestosql.testing.datatype.DataType.smallintDataType;
import static io.prestosql.testing.datatype.DataType.tinyintDataType;
import static io.prestosql.testing.datatype.DataType.varbinaryDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class TestPhoenixSqlTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingPhoenixServer phoenixServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        phoenixServer = TestingPhoenixServer.getInstance();
        return createPhoenixQueryRunner(phoenixServer);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        TestingPhoenixServer.shutDown();
    }

    @Test
    public void testBasicTypes()
    {
        DataTypeTest.create()
                .addRoundTrip(booleanDataType(), true)
                .addRoundTrip(booleanDataType(), false)
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(tinyintDataType(), (byte) 5)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases(varbinaryDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("test_varbinary"));

        varbinaryTestCases(phoenixVarbinaryDataType())
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_varbinary"));
    }

    public static DataType<byte[]> phoenixVarbinaryDataType()
    {
        return dataType("varbinary", VarbinaryType.VARBINARY, value -> format("DECODE('%s', 'HEX')", base16().encode(value)), Function.identity());
    }

    private DataTypeTest varbinaryTestCases(DataType<byte[]> varbinaryDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(varbinaryDataType, "hello".getBytes(UTF_8))
                .addRoundTrip(varbinaryDataType, "Piękna łąka w 東京都".getBytes(UTF_8))
                .addRoundTrip(varbinaryDataType, "Bag full of 💰".getBytes(UTF_16LE))
                .addRoundTrip(varbinaryDataType, null)
                .addRoundTrip(varbinaryDataType, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 13, -7, 54, 122, -89, 0, 0, 0});
    }

    @Test
    public void testVarchar()
    {
        DataTypeTest varcharTypeTest = stringDataTypeTest(DataType::varcharDataType)
                .addRoundTrip(varcharDataType(10485760), "text_f")
                .addRoundTrip(varcharDataType(), "unbounded");

        varcharTypeTest
                .execute(getQueryRunner(), prestoCreateAsSelect("test_varchar"));

        varcharTypeTest
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_varchar"));
    }

    private DataTypeTest stringDataTypeTest(Function<Integer, DataType<String>> dataTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(dataTypeFactory.apply(10), "text_a")
                .addRoundTrip(dataTypeFactory.apply(255), "text_b")
                .addRoundTrip(dataTypeFactory.apply(65535), "text_d");
    }

    @Test
    public void testChar()
    {
        stringDataTypeTest(DataType::charDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_char"));

        stringDataTypeTest(DataType::charDataType)
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_char"));
    }

    @Test
    public void testDecimal()
    {
        decimalTests(DataType::decimalDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_decimal"));

        decimalTests(TestPhoenixSqlTypeMapping::phoenixDecimalDataType)
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_decimal"));
    }

    private DataTypeTest decimalTests(BiFunction<Integer, Integer, DataType<BigDecimal>> dataTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(dataTypeFactory.apply(3, 0), new BigDecimal("193"))
                .addRoundTrip(dataTypeFactory.apply(3, 0), new BigDecimal("19"))
                .addRoundTrip(dataTypeFactory.apply(3, 0), new BigDecimal("-193"))
                .addRoundTrip(dataTypeFactory.apply(3, 1), new BigDecimal("10.0"))
                .addRoundTrip(dataTypeFactory.apply(3, 1), new BigDecimal("10.1"))
                .addRoundTrip(dataTypeFactory.apply(3, 1), new BigDecimal("-10.1"))
                .addRoundTrip(dataTypeFactory.apply(4, 2), new BigDecimal("2"))
                .addRoundTrip(dataTypeFactory.apply(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(dataTypeFactory.apply(24, 2), new BigDecimal("2"))
                .addRoundTrip(dataTypeFactory.apply(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(dataTypeFactory.apply(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(dataTypeFactory.apply(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(dataTypeFactory.apply(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(dataTypeFactory.apply(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(dataTypeFactory.apply(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(dataTypeFactory.apply(38, 0), new BigDecimal("-27182818284590452353602874713526624977"));
    }

    private static DataType<BigDecimal> phoenixDecimalDataType(int precision, int scale)
    {
        String databaseType = format("decimal(%s, %s)", precision, scale);
        return dataType(
                databaseType,
                createDecimalType(precision, scale),
                bigDecimal -> format("CAST(%s AS %s)", bigDecimal, databaseType),
                bigDecimal -> bigDecimal.setScale(scale, UNNECESSARY));
    }

    @Test
    public void testDate()
    {
        // Note: there is identical test for MySQL

        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        DataTypeTest prestoTestCases = dateTests(
                dateOfLocalTimeChangeForwardAtMidnightInJvmZone,
                dateOfLocalTimeChangeForwardAtMidnightInSomeZone,
                dateOfLocalTimeChangeBackwardAtMidnightInSomeZone,
                dateDataType());

        DataTypeTest phoenixTestCases = dateTests(
                dateOfLocalTimeChangeForwardAtMidnightInJvmZone,
                dateOfLocalTimeChangeForwardAtMidnightInSomeZone,
                dateOfLocalTimeChangeBackwardAtMidnightInSomeZone,
                phoenixDateDataType())
                .addRoundTrip(primaryKey(), 1);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            prestoTestCases.execute(getQueryRunner(), session, prestoCreateAsSelect("test_date"));
            phoenixTestCases.execute(getQueryRunner(), session, phoenixCreateAndInsert("tpch.test_date"));
        }
    }

    @Test
    public void testArray()
    {
        // basic types
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(booleanDataType()), asList(true, false))
                .addRoundTrip(arrayDataType(bigintDataType()), asList(123_456_789_012L))
                .addRoundTrip(arrayDataType(integerDataType()), asList(1, 2, 1_234_567_890))
                .addRoundTrip(arrayDataType(smallintDataType()), asList((short) 32_456))
                .addRoundTrip(arrayDataType(doubleDataType()), asList(123.45d))
                .addRoundTrip(arrayDataType(realDataType()), asList(123.45f))
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_basic"));

        arrayDateTest(TestPhoenixSqlTypeMapping::arrayDataType, dateDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_date"));
        arrayDateTest(TestPhoenixSqlTypeMapping::phoenixArrayDataType, phoenixDateDataType())
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_array_date"));

        arrayDecimalTest(TestPhoenixSqlTypeMapping::arrayDataType, DataType::decimalDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_decimal"));
        arrayDecimalTest(TestPhoenixSqlTypeMapping::phoenixArrayDataType, TestPhoenixSqlTypeMapping::phoenixDecimalDataType)
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_array_decimal"));

        arrayStringDataTypeTest(TestPhoenixSqlTypeMapping::arrayDataType, DataType::charDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_char"));
        arrayStringDataTypeTest(TestPhoenixSqlTypeMapping::phoenixArrayDataType, DataType::charDataType)
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_array_char"));

        arrayStringDataTypeTest(TestPhoenixSqlTypeMapping::arrayDataType, DataType::varcharDataType)
                .addRoundTrip(arrayDataType(varcharDataType(10485760)), asList("text_f"))
                .addRoundTrip(arrayDataType(varcharDataType()), asList("unbounded"))
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_varchar"));
        arrayStringDataTypeTest(TestPhoenixSqlTypeMapping::phoenixArrayDataType, DataType::varcharDataType)
                .addRoundTrip(phoenixArrayDataType(varcharDataType(10485760)), asList("text_f"))
                .addRoundTrip(phoenixArrayDataType(varcharDataType()), asList("unbounded"))
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_array_varchar"));
    }

    @Test
    public void testArrayNulls()
    {
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(booleanDataType()), null)
                .addRoundTrip(arrayDataType(varcharDataType()), singletonList(null))
                .addRoundTrip(arrayDataType(varcharDataType()), asList("foo", null, "bar", null))
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_nulls"));
    }

    private DataTypeTest arrayDecimalTest(Function<DataType<BigDecimal>, DataType<List<BigDecimal>>> arrayTypeFactory, BiFunction<Integer, Integer, DataType<BigDecimal>> decimalTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(decimalTypeFactory.apply(3, 0)), asList(new BigDecimal("193"), new BigDecimal("19"), new BigDecimal("-193")))
                .addRoundTrip(arrayTypeFactory.apply(decimalTypeFactory.apply(3, 1)), asList(new BigDecimal("10.0"), new BigDecimal("10.1"), new BigDecimal("-10.1")))
                .addRoundTrip(arrayTypeFactory.apply(decimalTypeFactory.apply(4, 2)), asList(new BigDecimal("2"), new BigDecimal("2.3")))
                .addRoundTrip(arrayTypeFactory.apply(decimalTypeFactory.apply(24, 2)), asList(new BigDecimal("2"), new BigDecimal("2.3"), new BigDecimal("123456789.3")))
                .addRoundTrip(arrayTypeFactory.apply(decimalTypeFactory.apply(24, 4)), asList(new BigDecimal("12345678901234567890.31")))
                .addRoundTrip(arrayTypeFactory.apply(decimalTypeFactory.apply(30, 5)), asList(new BigDecimal("3141592653589793238462643.38327"), new BigDecimal("-3141592653589793238462643.38327")))
                .addRoundTrip(arrayTypeFactory.apply(decimalTypeFactory.apply(38, 0)), asList(
                        new BigDecimal("27182818284590452353602874713526624977"),
                        new BigDecimal("-27182818284590452353602874713526624977")));
    }

    private DataTypeTest arrayStringDataTypeTest(Function<DataType<String>, DataType<List<String>>> arrayTypeFactory, Function<Integer, DataType<String>> dataTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(10)), asList("text_a"))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(255)), asList("text_b"))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(65535)), asList("text_d"));
    }

    private DataTypeTest arrayDateTest(Function<DataType<LocalDate>, DataType<List<LocalDate>>> arrayTypeFactory, DataType<LocalDate> dateDataType)
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(dateDataType), asList(LocalDate.of(1952, 4, 3))) // before epoch
                .addRoundTrip(arrayTypeFactory.apply(dateDataType), asList(LocalDate.of(1970, 1, 1)))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType), asList(LocalDate.of(1970, 2, 3)))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType), asList(LocalDate.of(2017, 7, 1))) // summer on northern hemisphere (possible DST)
                .addRoundTrip(arrayTypeFactory.apply(dateDataType), asList(LocalDate.of(2017, 1, 1))) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(arrayTypeFactory.apply(dateDataType), asList(dateOfLocalTimeChangeForwardAtMidnightInJvmZone))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType), asList(dateOfLocalTimeChangeForwardAtMidnightInSomeZone))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType), asList(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone));
    }

    private DataTypeTest dateTests(
            LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone,
            LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone,
            LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone,
            DataType<LocalDate> dateDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(dateDataType, LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(dateDataType, LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType, LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType, LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType, LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType, dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType, dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, format("ARRAY(%s)", elementType.getInsertType()));
    }

    private static <E> DataType<List<E>> phoenixArrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, elementType.getInsertType() + " ARRAY");
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType, String insertType)
    {
        return dataType(
                insertType,
                new ArrayType(elementType.getPrestoResultType()),
                valuesList -> "ARRAY" + valuesList.stream().map(elementType::toLiteral).collect(toList()),
                valuesList -> valuesList == null ? null : valuesList.stream().map(elementType::toPrestoQueryResult).collect(toList()));
    }

    public static DataType<LocalDate> phoenixDateDataType()
    {
        return dataType(
                "date",
                DATE,
                value -> format("TO_DATE('%s', 'yyyy-MM-dd', 'local')", DateTimeFormatter.ofPattern("yyyy-MM-dd").format(value)),
                identity());
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    private DataType<Integer> primaryKey()
    {
        return dataType("integer primary key", INTEGER, Object::toString, Function.identity());
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    private DataSetup phoenixCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new PhoenixSqlExecutor(phoenixServer.getJdbcUrl()), tableNamePrefix);
    }
}
