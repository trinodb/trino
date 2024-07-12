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
package io.trino.sql.query;

import io.trino.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static io.trino.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static io.trino.server.testing.TestingTrinoServer.SESSION_START_TIME_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestAggregation
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testQuantifiedComparison()
    {
        assertThat(assertions.query("SELECT v > ALL (VALUES 1) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .failure().hasMessageContaining("must be an aggregate expression or appear in GROUP BY clause");

        assertThat(assertions.query("SELECT v > ANY (VALUES 1) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .failure().hasMessageContaining("must be an aggregate expression or appear in GROUP BY clause");

        assertThat(assertions.query("SELECT v > SOME (VALUES 1) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .failure().hasMessageContaining("must be an aggregate expression or appear in GROUP BY clause");

        assertThat(assertions.query("SELECT count_if(v > ALL (VALUES 0, 1)) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .matches("VALUES BIGINT '1'");

        assertThat(assertions.query("SELECT count_if(v > ANY (VALUES 0, 1)) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .matches("VALUES BIGINT '2'");

        assertThat(assertions.query("SELECT 1 > ALL (VALUES k) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .failure().hasMessageContaining("line 1:17: Given correlated subquery is not supported");
    }

    @Test
    void testSpecialDateTimeFunctionsInAggregation()
    {
        Session session = Session.builder(assertions.getDefaultSession())
                .setStart(ZonedDateTime.of(2024, 3, 12, 12, 24, 0, 0, ZoneId.of("Pacific/Apia")).toInstant())
                .setSystemProperty(SESSION_START_TIME_PROPERTY, ZonedDateTime.of(2024, 3, 12, 12, 24, 0, 0, ZoneId.of("Pacific/Apia")).toInstant().toString())
                .build();

        assertThat(assertions.query(
                session,
                """
                        WITH t(x) AS (VALUES 1)
                        SELECT max(x), current_timestamp, current_date, current_time, localtimestamp, localtime
                        FROM t
                        """))
                .matches(
                        """
                                VALUES (
                                    1,
                                    TIMESTAMP '2024-03-12 12:24:0.000 Pacific/Apia',
                                    DATE '2024-03-12',
                                    TIME '12:24:0.000+13:00',
                                    TIMESTAMP '2024-03-12 12:24:0.000',
                                    TIME '12:24:0.000')
                                """);
    }

    /**
     * Regression test for <a href="https://github.com/trinodb/trino/issues/21002">#21002</a>
     */
    @Test
    public void testAggregationMaskOnDictionaryInput()
    {
        assertThat(assertions.query(
                """
                        SELECT
                            max(update_ts) FILTER (WHERE step_type = 'Rest')
                        FROM (VALUES
                                ('cell_id', 'Rest', TIMESTAMP '2005-09-10 13:31:00.123 Europe/Warsaw'),
                                ('cell_id', 'Rest', TIMESTAMP '2005-09-10 13:31:00.123 Europe/Warsaw')
                            ) AS t(cell_id, step_type, update_ts)
                        -- UNNEST to produce DictionaryBlock
                        CROSS JOIN UNNEST (sequence(1, 1000)) AS a(e)
                        GROUP BY cell_id
                        """))
                .matches("VALUES TIMESTAMP '2005-09-10 13:31:00.123 Europe/Warsaw'");
    }

    /**
     * Regression test for <a href="https://github.com/trinodb/trino/issues/21099">#21099</a>
     */
    @Test
    public void testLongDecimalPartialAggregation()
    {
        Session session = Session.builder(assertions.getDefaultSession())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .build();
        assertThat(assertions.query(session, """
                -- sum and avg likely use different aggregation state classes
                SELECT l.i, sum(v), avg(v)
                FROM (VALUES 1, 2, 2, 3, 3, 3) l(i)
                JOIN (VALUES
                    (1, DECIMAL '12345678901234567890.1234567890'),
                    (1, DECIMAL '11111111111111111111.1234567890'),
                    (2, DECIMAL '22222222222222222222.1234567890'),
                    (3, DECIMAL '33333333333333333333.1234567890'),
                    (3, DECIMAL '10101010101010101010.0987654321'),
                    (7, DECIMAL '77777777777777777777.1234567890')) r(i, v) ON l.i = r.i
                GROUP BY l.i
                """))
                .matches("""
                        SELECT i, CAST(s AS decimal(38, 10)), v
                        FROM (VALUES
                            (1, DECIMAL '23456790012345679001.2469135780', DECIMAL '11728395006172839500.6234567890'),
                            (2, DECIMAL '44444444444444444444.2469135780', DECIMAL '22222222222222222222.1234567890'),
                            (3, DECIMAL '130303030303030303029.6666666633', DECIMAL '21717171717171717171.6111111106')) t(i, s, v)
                        """);
    }

    @Test
    void testCountDistinctOverConstant()
    {
        assertThat(assertions.query("SELECT count(DISTINCT 'x'), count(*) FROM (VALUES 1, 2, 3)"))
                .matches("VALUES (BIGINT '1', BIGINT '3')");
    }
}
