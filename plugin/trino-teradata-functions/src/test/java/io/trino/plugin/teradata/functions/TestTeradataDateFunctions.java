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
package io.trino.plugin.teradata.functions;

import io.trino.spi.type.DateType;
import io.trino.spi.type.SqlDate;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTeradataDateFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions(testSessionBuilder()
                .setCatalog("catalog")
                .setSchema("schema")
                .build());
        assertions.addPlugin(new TeradataFunctionsPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testMinimalToDate()
    {
        assertThat(assertions.function("to_date", "'1988/04/08'", "'yyyy/mm/dd'"))
                .hasType(DateType.DATE)
                .isEqualTo(new SqlDate(toIntExact(LocalDate.of(1988, 4, 8).toEpochDay())));

        assertThat(assertions.function("to_date", "'04-08-1988'", "'mm-dd-yyyy'"))
                .hasType(DateType.DATE)
                .isEqualTo(new SqlDate(toIntExact(LocalDate.of(1988, 4, 8).toEpochDay())));

        assertThat(assertions.function("to_date", "'04.1988,08'", "'mm.yyyy,dd'"))
                .hasType(DateType.DATE)
                .isEqualTo(new SqlDate(toIntExact(LocalDate.of(1988, 4, 8).toEpochDay())));

        assertThat(assertions.function("to_date", "';198804:08'", "';yyyymm:dd'"))
                .hasType(DateType.DATE)
                .isEqualTo(new SqlDate(toIntExact(LocalDate.of(1988, 4, 8).toEpochDay())));
    }

    @Test
    public void testMinimalToTimestamp()
    {
        assertThat(assertions.function("to_timestamp", "'1988/04/08'", "'yyyy/mm/dd'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 0, 0, 0, 0));

        assertThat(assertions.function("to_timestamp", "'04-08-1988'", "'mm-dd-yyyy'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 0, 0, 0, 0));

        assertThat(assertions.function("to_timestamp", "'04.1988,08'", "'mm.yyyy,dd'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 0, 0, 0, 0));

        assertThat(assertions.function("to_timestamp", "';198804:08'", "';yyyymm:dd'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 0, 0, 0, 0));

        assertThat(assertions.function("to_timestamp", "'1988/04/08 2'", "'yyyy/mm/dd hh'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 2, 0, 0, 0));

        assertThat(assertions.function("to_timestamp", "'1988/04/08 14'", "'yyyy/mm/dd hh24'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 14, 0, 0, 0));

        assertThat(assertions.function("to_timestamp", "'1988/04/08 14:15'", "'yyyy/mm/dd hh24:mi'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 14, 15, 0, 0));

        assertThat(assertions.function("to_timestamp", "'1988/04/08 14:15:16'", "'yyyy/mm/dd hh24:mi:ss'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 14, 15, 16, 0));

        assertThat(assertions.function("to_timestamp", "'1988/04/08 2:3:4'", "'yyyy/mm/dd hh24:mi:ss'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 2, 3, 4, 0));

        assertThat(assertions.function("to_timestamp", "'1988/04/08 02:03:04'", "'yyyy/mm/dd hh24:mi:ss'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 1988, 4, 8, 2, 3, 4, 0));
    }

    @Test
    public void testMinimalToChar()
    {
        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04'", "'yyyy/mm/dd hh24:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 14:15:16'", "'yyyy/mm/dd hh24:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 14:15:16");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 14:15:16 +02:09'", "'yyyy/mm/dd hh24:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 14:15:16");

        assertThat(assertions.function("to_char", "DATE '1988-04-08'", "'yyyy/mm/dd hh24:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 00:00:00");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.0'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.00'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.0000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.00000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.000000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.0000000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.00000000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.000000000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.0000000000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.00000000000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");

        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08 02:03:04.000000000000'", "'yyyy/mm/dd hh:mi:ss'"))
                .hasType(VARCHAR)
                .isEqualTo("1988/04/08 02:03:04");
    }

    @Test
    public void testYY()
    {
        assertThat(assertions.function("to_char", "TIMESTAMP '1988-04-08'", "'yy'"))
                .hasType(VARCHAR)
                .isEqualTo("88");

        assertThat(assertions.function("to_timestamp", "'88/04/08'", "'yy/mm/dd'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 2088, 4, 8, 0, 0, 0, 0));

        assertThat(assertions.function("to_date", "'88/04/08'", "'yy/mm/dd'"))
                .hasType(DateType.DATE)
                .isEqualTo(new SqlDate(toIntExact(LocalDate.of(2088, 4, 8).toEpochDay())));
    }

    @Test
    public void testWhitespace()
    {
        assertThat(assertions.expression("to_date('8 04 1988','dd mm yyyy')"))
                .hasType(DateType.DATE)
                .isEqualTo(new SqlDate(toIntExact(LocalDate.of(1988, 4, 8).toEpochDay())));
    }
}
