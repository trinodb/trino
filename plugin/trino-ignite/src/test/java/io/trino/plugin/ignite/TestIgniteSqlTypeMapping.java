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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.ignite.IgniteQueryRunner.createIgniteQueryRunner;
import static io.trino.plugin.ignite.IgniteQueryRunner.createSession;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.datatype.DataType.bigintDataType;
import static io.trino.testing.datatype.DataType.dateDataType;
import static io.trino.type.UuidType.UUID;

public class TestIgniteSqlTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingIgniteServer igniteServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.igniteServer = closeAfterClass(new TestingIgniteServer());
        return createIgniteQueryRunner(
                igniteServer,
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Test
    public void testBasicTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .addRoundTrip("bigint", "123456789012", BIGINT)
                .addRoundTrip("int", "123456789", INTEGER)
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_basic_types"));
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("real", "'NaN'::real", REAL, "CAST(nan() AS real)")
                .addRoundTrip("real", "'-Infinity'::real", REAL, "CAST(-infinity() AS real)")
                .addRoundTrip("real", "'+Infinity'::real", REAL, "CAST(+infinity() AS real)")
                .execute(getQueryRunner(), igniteCreateAndInsert("ignite_test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("real", "nan()", REAL, "CAST(nan() AS real)")
                .addRoundTrip("real", "-infinity()", REAL, "CAST(-infinity() AS real)")
                .addRoundTrip("real", "+infinity()", REAL, "CAST(+infinity() AS real)")
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double precision", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double precision", "'NaN'::double precision", DOUBLE, "nan()")
                .addRoundTrip("double precision", "'+Infinity'::double precision", DOUBLE, "+infinity()")
                .addRoundTrip("double precision", "'-Infinity'::double precision", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), igniteCreateAndInsert("ignite_test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "nan()", DOUBLE, "nan()")
                .addRoundTrip("double", "+infinity()", DOUBLE, "+infinity()")
                .addRoundTrip("double", "-infinity()", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), trinoCreateAndInsert(createSession(), "trino_test_double"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('123456789.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST('12345678901234567890.31' AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    public void testBinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "987654321123", BIGINT)
                .addRoundTrip("binary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("binary", "X''", VARBINARY, "X''")
                .addRoundTrip("binary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("binary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("binary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("binary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("binary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_varbinary"));

        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "987654321123", BIGINT)
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X''", VARBINARY, "X''")
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varbinary"));
    }

    @Test
    public void testUuid()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("uuid", "CAST ('114514ea-0601-1981-1142-e9b55b0abd6d' as uuid)", UUID, "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS uuid)")
                .addRoundTrip("uuid", "CAST ('114514ea-0601-1981-1142-e9b55b0abd6e' as uuid)", UUID, "CAST('114514ea-0601-1981-1142-e9b55b0abd6e' AS uuid)")
                .addRoundTrip("uuid", "CAST(NULL AS uuid)", UUID, "cast(NULL as uuid)")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_uuid"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_uuid"));
    }

    @Test
    public void testDate()
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

        DataTypeTest testCases = DataTypeTest.create(true)
                .addRoundTrip(bigintDataType(), 123L)
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, igniteCreateAndInsert("test_date"));
            testCases.execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"));
        }
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return new TrinoCreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), createSession()), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new TrinoCreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup igniteCreateAndInsert(String tableNamePrefix)
    {
        return new IgniteCreateAndInsertDataSetup(new JdbcSqlExecutor(igniteServer.getJdbcUrl()), tableNamePrefix);
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
}
