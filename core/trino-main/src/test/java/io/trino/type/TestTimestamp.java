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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTimestamp
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
    public void testCastFromVarcharContainingTimeZone()
    {
        assertThat(assertions.expression("CAST('2001-1-22 03:04:05.321 +07:09' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:05.321'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04:05 +07:09' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:05.000'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04 +07:09' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:00.000'");

        assertThat(assertions.expression("CAST('2001-1-22 +07:09' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 00:00:00.000'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04:05.321 Asia/Oral' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:05.321'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04:05 Asia/Oral' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:05.000'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04 Asia/Oral' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:00.000'");

        assertThat(assertions.expression("CAST('2001-1-22 Asia/Oral' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 00:00:00.000'");
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "TIMESTAMP '2017-03-30 14:15:16.432'", "TIMESTAMP '2016-03-29 03:04:05.321'"))
                .matches("INTERVAL '366 11:11:11.111' DAY TO SECOND");

        assertThat(assertions.operator(SUBTRACT, "TIMESTAMP '2016-03-29 03:04:05.321'", "TIMESTAMP '2017-03-30 14:15:16.432'"))
                .matches("INTERVAL '-366 11:11:11.111' DAY TO SECOND");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-11'"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-20'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-20'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.111'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-23'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.111'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-23'"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.111'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.111'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.322'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.311'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.312'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.333'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.111'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreatest()
    {
        assertThat(assertions.function("greatest", "TIMESTAMP '2013-03-30 01:05'", "TIMESTAMP '2012-03-30 01:05'"))
                .matches("TIMESTAMP '2013-03-30 01:05'");

        assertThat(assertions.function("greatest", "TIMESTAMP '2013-03-30 01:05'", "TIMESTAMP '2012-03-30 01:05'", "TIMESTAMP '2012-05-01 01:05'"))
                .matches("TIMESTAMP '2013-03-30 01:05'");
    }

    @Test
    public void testLeast()
    {
        assertThat(assertions.function("least", "TIMESTAMP '2013-03-30 01:05'", "TIMESTAMP '2012-03-30 01:05'"))
                .matches("TIMESTAMP '2012-03-30 01:05'");

        assertThat(assertions.function("least", "TIMESTAMP '2013-03-30 01:05'", "TIMESTAMP '2012-03-30 01:05'", "TIMESTAMP '2012-05-01 01:05'"))
                .matches("TIMESTAMP '2012-03-30 01:05'");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(null AS TIMESTAMP)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "TIMESTAMP '2012-03-30 01:05'"))
                .isEqualTo(false);
    }
}
