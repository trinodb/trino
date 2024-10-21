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
package io.trino.plugin.singlestore;

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.SqlDataTypeTest;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.time.ZoneOffset.UTC;

final class TestSingleStoreTypeMapping
        extends BaseSingleStoreTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        singleStoreServer = closeAfterClass(new TestingSingleStoreServer());
        return SingleStoreQueryRunner.builder(singleStoreServer).build();
    }

    @Test
    void testUnsupportedTinyint()
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-129", TINYINT, "TINYINT '-128'")
                .addRoundTrip("tinyint", "128", TINYINT, "TINYINT '127'")
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.test_unsupported_tinyint"));
    }

    @Test
    void testUnsupportedSmallint()
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32769", SMALLINT, "SMALLINT '-32768'")
                .addRoundTrip("smallint", "32768", SMALLINT, "SMALLINT '32767'")
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.test_unsupported_smallint"));
    }

    @Test
    void testUnsupportedInteger()
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483649", INTEGER, "INTEGER '-2147483648'")
                .addRoundTrip("integer", "2147483648", INTEGER, "INTEGER '2147483647'")
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.test_unsupported_integer"));
    }

    @Test
    void testUnsupportedBigint()
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775809", BIGINT, "BIGINT '-9223372036854775808'")
                .addRoundTrip("bigint", "9223372036854775808", BIGINT, "BIGINT '9223372036854775807'")
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.test_unsupported_bigint"));
    }

    @Test
    void testOlderDate()
    {
        testOlderDate(UTC);
        testOlderDate(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testOlderDate(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testOlderDate(ZoneId.of("Asia/Kathmandu"));
        testOlderDate(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testOlderDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("date", "CAST('0000-01-01' AS date)", DATE, "DATE '0000-01-01'")
                .addRoundTrip("date", "CAST('0001-01-01' AS date)", DATE, "DATE '0001-01-01'")
                .execute(getQueryRunner(), session, singleStoreCreateAndInsert("tpch.test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    @Test
    void testSingleStoreCreatedParameterizedVarcharUnicodeEmoji()
    {
        // SingleStore version >= 7.5 supports utf8mb4, but older versions store an empty character for a 4 bytes character
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(1) " + CHARACTER_SET_UTF8, "'😂'", createVarcharType(1), "CAST('' AS varchar(1))")
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.singlestore_test_parameterized_varchar_unicode"));
    }
}
