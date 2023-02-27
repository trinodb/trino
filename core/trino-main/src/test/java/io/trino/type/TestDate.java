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
package io.trino.type;

import io.trino.Session;
import io.trino.spi.type.SqlDate;
import io.trino.sql.query.QueryAssertions;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.TimeUnit;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDate
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testLiteral()
    {
        long millis = new DateTime(2001, 1, 22, 0, 0, UTC).getMillis();
        assertThat(assertions.expression("DATE '2001-1-22'"))
                .hasType(DATE)
                .isEqualTo(new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis)));
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "DATE '2001-1-22'", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DATE '2001-1-22'", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DATE '2001-1-22'", "DATE '2001-1-23'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DATE '2001-1-22'", "DATE '2001-1-11'"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-22'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "DATE '2001-1-22'", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DATE '2001-1-22'", "DATE '2001-1-22'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DATE '2001-1-22'", "DATE '2001-1-20'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DATE '2001-1-22'", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DATE '2001-1-22'", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DATE '2001-1-22'", "DATE '2001-1-20'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-22'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-23'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-23'"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-11'")
                .binding("high", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-11'")
                .binding("high", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-22'")
                .binding("high", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-22'")
                .binding("high", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-11'")
                .binding("high", "DATE '2001-1-12'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-23'")
                .binding("high", "DATE '2001-1-24'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-23'")
                .binding("high", "DATE '2001-1-11'"))
                .isEqualTo(false);
    }

    @Test
    public void testCastToTimestamp()
    {
        assertThat(assertions.expression("CAST(DATE '2001-1-22' AS timestamp)"))
                .matches("TIMESTAMP '2001-01-22 00:00:00.000'");
    }

    @Test
    public void testCastToTimestampWithTimeZone()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("Europe/Berlin"))
                .build();

        assertThat(assertions.expression("CAST(DATE '2001-1-22' AS timestamp with time zone)", session))
                .matches("TIMESTAMP '2001-01-22 00:00:00.000 Europe/Berlin'");
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("CAST(DATE '2001-1-22' AS varchar)"))
                .hasType(VARCHAR)
                .isEqualTo("2001-01-22");
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("CAST('2001-1-22' AS date)"))
                .matches("DATE '2001-01-22'");

        assertThat(assertions.expression("CAST('\n\t 2001-1-22' AS date)"))
                .matches("DATE '2001-01-22'");

        assertThat(assertions.expression("CAST('2001-1-22 \t\n' AS date)"))
                .matches("DATE '2001-01-22'");

        assertThat(assertions.expression("CAST('\n\t 2001-1-22 \t\n' AS date)"))
                .matches("DATE '2001-01-22'");
    }

    @Test
    public void testGreatest()
    {
        assertThat(assertions.function("greatest", "DATE '2013-03-30'", "DATE '2012-05-23'"))
                .matches("DATE '2013-03-30'");

        assertThat(assertions.function("greatest", "DATE '2013-03-30'", "DATE '2012-05-23'", "DATE '2012-06-01'"))
                .matches("DATE '2013-03-30'");
    }

    @Test
    public void testLeast()
    {
        assertThat(assertions.function("least", "DATE '2013-03-30'", "DATE '2012-05-23'"))
                .matches("DATE '2012-05-23'");

        assertThat(assertions.function("least", "DATE '2013-03-30'", "DATE '2012-05-23'", "DATE '2012-06-01'"))
                .matches("DATE '2012-05-23'");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(NULL AS DATE)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "DATE '2013-10-27'"))
                .isEqualTo(false);
    }
}
