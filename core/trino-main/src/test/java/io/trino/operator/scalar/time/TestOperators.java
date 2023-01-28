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
package io.trino.operator.scalar.time;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOperators
{
    protected QueryAssertions assertions;

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
    public void testEqual()
    {
        assertThat(assertions.expression("TIME '12:34:56' = TIME '12:34:56'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' = TIME '12:34:56.1'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' = TIME '12:34:56.12'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' = TIME '12:34:56.123'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' = TIME '12:34:56.1234'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' = TIME '12:34:56.12345'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' = TIME '12:34:56.123456'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' = TIME '12:34:56.1234567'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' = TIME '12:34:56.12345678'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' = TIME '12:34:56.123456789'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' = TIME '12:34:56.1234567890'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' = TIME '12:34:56.12345678901'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' = TIME '12:34:56.123456789012'")).isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        // false cases
        assertThat(assertions.expression("TIME '12:34:56' <> TIME '12:34:56'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1' <> TIME '12:34:56.1'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12' <> TIME '12:34:56.12'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123' <> TIME '12:34:56.123'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234' <> TIME '12:34:56.1234'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345' <> TIME '12:34:56.12345'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456' <> TIME '12:34:56.123456'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567' <> TIME '12:34:56.1234567'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678' <> TIME '12:34:56.12345678'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789' <> TIME '12:34:56.123456789'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' <> TIME '12:34:56.1234567890'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' <> TIME '12:34:56.12345678901'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' <> TIME '12:34:56.123456789012'")).isEqualTo(false);

        // difference in the high-order data
        assertThat(assertions.expression("TIME '12:34:56' <> TIME '22:34:56'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' <> TIME '22:34:56.1'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' <> TIME '22:34:56.12'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' <> TIME '22:34:56.123'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' <> TIME '22:34:56.1234'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' <> TIME '22:34:56.12345'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' <> TIME '22:34:56.123456'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' <> TIME '22:34:56.1234567'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' <> TIME '22:34:56.12345678'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' <> TIME '22:34:56.123456789'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' <> TIME '22:34:56.1234567890'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' <> TIME '22:34:56.12345678901'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' <> TIME '22:34:56.123456789012'")).isEqualTo(true);

        // difference in the low-order data
        assertThat(assertions.expression("TIME '12:34:56' <> TIME '12:34:57'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' <> TIME '12:34:56.2'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' <> TIME '12:34:56.13'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' <> TIME '12:34:56.124'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' <> TIME '12:34:56.1235'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' <> TIME '12:34:56.12346'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' <> TIME '12:34:56.123457'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' <> TIME '12:34:56.1234568'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' <> TIME '12:34:56.12345679'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' <> TIME '12:34:56.123456781'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' <> TIME '12:34:56.1234567891'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' <> TIME '12:34:56.12345678902'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' <> TIME '12:34:56.123456789013'")).isEqualTo(true);
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.expression("TIME '12:34:56' IS DISTINCT FROM TIME '12:34:56'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1' IS DISTINCT FROM TIME '12:34:56.1'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12' IS DISTINCT FROM TIME '12:34:56.12'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123' IS DISTINCT FROM TIME '12:34:56.123'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234' IS DISTINCT FROM TIME '12:34:56.1234'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345' IS DISTINCT FROM TIME '12:34:56.12345'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456' IS DISTINCT FROM TIME '12:34:56.123456'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567' IS DISTINCT FROM TIME '12:34:56.1234567'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678' IS DISTINCT FROM TIME '12:34:56.12345678'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789' IS DISTINCT FROM TIME '12:34:56.123456789'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' IS DISTINCT FROM TIME '12:34:56.1234567890'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' IS DISTINCT FROM TIME '12:34:56.12345678901'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' IS DISTINCT FROM TIME '12:34:56.123456789012'")).isEqualTo(false);

        assertThat(assertions.expression("TIME '12:34:56' IS DISTINCT FROM TIME '12:34:55'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' IS DISTINCT FROM TIME '12:34:56.0'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' IS DISTINCT FROM TIME '12:34:56.11'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' IS DISTINCT FROM TIME '12:34:56.122'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' IS DISTINCT FROM TIME '12:34:56.1233'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' IS DISTINCT FROM TIME '12:34:56.12344'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' IS DISTINCT FROM TIME '12:34:56.123455'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' IS DISTINCT FROM TIME '12:34:56.1234566'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' IS DISTINCT FROM TIME '12:34:56.12345677'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' IS DISTINCT FROM TIME '12:34:56.123456788'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' IS DISTINCT FROM TIME '12:34:56.1234567899'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' IS DISTINCT FROM TIME '12:34:56.12345678900'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' IS DISTINCT FROM TIME '12:34:56.123456789011'")).isEqualTo(true);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.expression("TIME '12:34:56' < TIME '12:34:57'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' < TIME '12:34:56.2'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' < TIME '12:34:56.13'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' < TIME '12:34:56.124'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' < TIME '12:34:56.1235'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' < TIME '12:34:56.12346'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' < TIME '12:34:56.123457'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' < TIME '12:34:56.1234568'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' < TIME '12:34:56.12345679'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' < TIME '12:34:56.123456790'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' < TIME '12:34:56.1234567891'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' < TIME '12:34:56.12345678902'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' < TIME '12:34:56.123456789013'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIME '12:34:56' < TIME '12:34:56'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1' < TIME '12:34:56.1'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12' < TIME '12:34:56.12'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123' < TIME '12:34:56.123'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234' < TIME '12:34:56.1234'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345' < TIME '12:34:56.12345'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456' < TIME '12:34:56.123456'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567' < TIME '12:34:56.1234567'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678' < TIME '12:34:56.12345678'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789' < TIME '12:34:56.123456789'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' < TIME '12:34:56.1234567890'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' < TIME '12:34:56.12345678901'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' < TIME '12:34:56.123456789012'")).isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("TIME '12:34:56' > TIME '12:34:55'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' > TIME '12:34:55.9'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' > TIME '12:34:56.11'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' > TIME '12:34:56.122'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' > TIME '12:34:56.1233'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' > TIME '12:34:56.12344'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' > TIME '12:34:56.123455'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' > TIME '12:34:56.1234566'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' > TIME '12:34:56.12345677'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' > TIME '12:34:56.123456788'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' > TIME '12:34:56.1234567889'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' > TIME '12:34:56.12345678899'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' > TIME '12:34:56.123456789011'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIME '12:34:56' > TIME '12:34:56'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1' > TIME '12:34:56.1'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12' > TIME '12:34:56.12'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123' > TIME '12:34:56.123'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234' > TIME '12:34:56.1234'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345' > TIME '12:34:56.12345'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456' > TIME '12:34:56.123456'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567' > TIME '12:34:56.1234567'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678' > TIME '12:34:56.12345678'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789' > TIME '12:34:56.123456789'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' > TIME '12:34:56.1234567890'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' > TIME '12:34:56.12345678901'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' > TIME '12:34:56.123456789012'")).isEqualTo(false);
    }

    @Test
    public void testLessThanOrEquals()
    {
        // equality
        assertThat(assertions.expression("TIME '12:34:56' <= TIME '12:34:56'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' <= TIME '12:34:56.1'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' <= TIME '12:34:56.12'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' <= TIME '12:34:56.123'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' <= TIME '12:34:56.1234'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' <= TIME '12:34:56.12345'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' <= TIME '12:34:56.123456'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' <= TIME '12:34:56.1234567'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' <= TIME '12:34:56.12345678'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' <= TIME '12:34:56.123456789'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' <= TIME '12:34:56.1234567890'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' <= TIME '12:34:56.12345678901'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' <= TIME '12:34:56.123456789012'")).isEqualTo(true);

        // less than
        assertThat(assertions.expression("TIME '12:34:56' <= TIME '12:34:57'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' <= TIME '12:34:56.2'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' <= TIME '12:34:56.13'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' <= TIME '12:34:56.124'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' <= TIME '12:34:56.1235'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' <= TIME '12:34:56.12346'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' <= TIME '12:34:56.123457'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' <= TIME '12:34:56.1234568'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' <= TIME '12:34:56.12345679'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' <= TIME '12:34:56.123456790'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' <= TIME '12:34:56.1234567891'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' <= TIME '12:34:56.12345678902'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' <= TIME '12:34:56.123456789013'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIME '12:34:56' <= TIME '12:34:55'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1' <= TIME '12:34:56.0'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12' <= TIME '12:34:56.11'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123' <= TIME '12:34:56.122'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234' <= TIME '12:34:56.1233'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345' <= TIME '12:34:56.12344'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456' <= TIME '12:34:56.123454'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567' <= TIME '12:34:56.1234566'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678' <= TIME '12:34:56.12345677'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789' <= TIME '12:34:56.123456788'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' <= TIME '12:34:56.1234567889'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' <= TIME '12:34:56.12345678900'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' <= TIME '12:34:56.123456789011'")).isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEquals()
    {
        // equality
        assertThat(assertions.expression("TIME '12:34:56' >= TIME '12:34:56'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' >= TIME '12:34:56.1'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' >= TIME '12:34:56.12'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' >= TIME '12:34:56.123'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' >= TIME '12:34:56.1234'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' >= TIME '12:34:56.12345'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' >= TIME '12:34:56.123456'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' >= TIME '12:34:56.1234567'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' >= TIME '12:34:56.12345678'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' >= TIME '12:34:56.123456789'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' >= TIME '12:34:56.1234567890'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' >= TIME '12:34:56.12345678901'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' >= TIME '12:34:56.123456789012'")).isEqualTo(true);

        // greater than
        assertThat(assertions.expression("TIME '12:34:56' >= TIME '12:34:55'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' >= TIME '12:34:55.9'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' >= TIME '12:34:56.11'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' >= TIME '12:34:56.122'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' >= TIME '12:34:56.1233'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' >= TIME '12:34:56.12344'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' >= TIME '12:34:56.123455'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' >= TIME '12:34:56.1234566'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' >= TIME '12:34:56.12345677'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' >= TIME '12:34:56.123456788'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' >= TIME '12:34:56.1234567889'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' >= TIME '12:34:56.12345678899'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' >= TIME '12:34:56.123456789011'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIME '12:34:56' >= TIME '12:34:57'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1' >= TIME '12:34:56.2'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12' >= TIME '12:34:56.13'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123' >= TIME '12:34:56.124'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234' >= TIME '12:34:56.1235'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345' >= TIME '12:34:56.12346'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456' >= TIME '12:34:56.123457'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567' >= TIME '12:34:56.1234568'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678' >= TIME '12:34:56.12345679'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789' >= TIME '12:34:56.1234567891'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' >= TIME '12:34:56.1234567891'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' >= TIME '12:34:56.12345678902'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' >= TIME '12:34:56.123456789013'")).isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("TIME '12:34:56' BETWEEN TIME '12:34:55' and TIME '12:34:57'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1' BETWEEN TIME '12:34:56.0' and TIME '12:34:56.2'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12' BETWEEN TIME '12:34:56.11' and TIME '12:34:56.13'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123' BETWEEN TIME '12:34:56.122' and TIME '12:34:56.124'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234' BETWEEN TIME '12:34:56.1233' and TIME '12:34:56.1235'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345' BETWEEN TIME '12:34:56.12344' and TIME '12:34:56.12346'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456' BETWEEN TIME '12:34:56.123455' and TIME '12:34:56.123457'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567' BETWEEN TIME '12:34:56.1234566' and TIME '12:34:56.1234568'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678' BETWEEN TIME '12:34:56.12345677' and TIME '12:34:56.12345679'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789' BETWEEN TIME '12:34:56.123456788' and TIME '12:34:56.123456790'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567890' BETWEEN TIME '12:34:56.1234567889' and TIME '12:34:56.1234567891'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678901' BETWEEN TIME '12:34:56.1234567890' and TIME '12:34:56.12345678902'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789012' BETWEEN TIME '12:34:56.123456789011' and TIME '12:34:56.123456789013'")).isEqualTo(true);
    }

    @Test
    public void testAddIntervalDayToSecond()
    {
        assertThat(assertions.expression("TIME '12:34:56' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.123'");
        assertThat(assertions.expression("TIME '12:34:56.1' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.223'");
        assertThat(assertions.expression("TIME '12:34:56.12' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.243'");
        assertThat(assertions.expression("TIME '12:34:56.123' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.246'");
        assertThat(assertions.expression("TIME '12:34:56.1234' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.2464'");
        assertThat(assertions.expression("TIME '12:34:56.12345' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.24645'");
        assertThat(assertions.expression("TIME '12:34:56.123456' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.246456'");
        assertThat(assertions.expression("TIME '12:34:56.1234567' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.2464567'");
        assertThat(assertions.expression("TIME '12:34:56.12345678' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.24645678'");
        assertThat(assertions.expression("TIME '12:34:56.123456789' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.246456789'");
        assertThat(assertions.expression("TIME '12:34:56.1234567890' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.2464567890'");
        assertThat(assertions.expression("TIME '12:34:56.12345678901' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.24645678901'");
        assertThat(assertions.expression("TIME '12:34:56.123456789012' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.246456789012'");

        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56'")).matches("TIME '12:34:57.123'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.1'")).matches("TIME '12:34:57.223'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.12'")).matches("TIME '12:34:57.243'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.123'")).matches("TIME '12:34:57.246'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.1234'")).matches("TIME '12:34:57.2464'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.12345'")).matches("TIME '12:34:57.24645'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.123456'")).matches("TIME '12:34:57.246456'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.1234567'")).matches("TIME '12:34:57.2464567'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.12345678'")).matches("TIME '12:34:57.24645678'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.123456789'")).matches("TIME '12:34:57.246456789'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.1234567890'")).matches("TIME '12:34:57.2464567890'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.12345678901'")).matches("TIME '12:34:57.24645678901'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.123456789012'")).matches("TIME '12:34:57.246456789012'");

        // carry
        assertThat(assertions.expression("TIME '12:59:59' + INTERVAL '1' SECOND")).matches("TIME '13:00:00.000'");
        assertThat(assertions.expression("TIME '12:59:59.999' + INTERVAL '0.001' SECOND")).matches("TIME '13:00:00.000'");

        // wrap-around
        assertThat(assertions.expression("TIME '12:34:56' + INTERVAL '13' HOUR")).matches("TIME '01:34:56.000'");
    }

    @Test
    public void testSubtractIntervalDayToSecond()
    {
        assertThat(assertions.expression("TIME '12:34:56' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:54.877'");
        assertThat(assertions.expression("TIME '12:34:56.1' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:54.977'");
        assertThat(assertions.expression("TIME '12:34:56.12' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:54.997'");
        assertThat(assertions.expression("TIME '12:34:56.123' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.000'");
        assertThat(assertions.expression("TIME '12:34:56.1234' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.0004'");
        assertThat(assertions.expression("TIME '12:34:56.12345' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.00045'");
        assertThat(assertions.expression("TIME '12:34:56.123456' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.000456'");
        assertThat(assertions.expression("TIME '12:34:56.1234567' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.0004567'");
        assertThat(assertions.expression("TIME '12:34:56.12345678' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.00045678'");
        assertThat(assertions.expression("TIME '12:34:56.123456789' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.000456789'");
        assertThat(assertions.expression("TIME '12:34:56.1234567890' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.0004567890'");
        assertThat(assertions.expression("TIME '12:34:56.12345678901' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.00045678901'");
        assertThat(assertions.expression("TIME '12:34:56.123456789012' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.000456789012'");

        // borrow
        assertThat(assertions.expression("TIME '13:00:00' - INTERVAL '1' SECOND")).matches("TIME '12:59:59.000'");
        assertThat(assertions.expression("TIME '13:00:00' - INTERVAL '0.001' SECOND")).matches("TIME '12:59:59.999'");

        // wrap-around
        assertThat(assertions.expression("TIME '12:34:56' - INTERVAL '13' HOUR")).matches("TIME '23:34:56.000'");
    }

    @Test
    public void testSubtract()
    {
        // round down, positive
        assertThat(assertions.expression("TIME '12:34:56' - TIME '12:34:55'")).matches("INTERVAL '1' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.2' - TIME '12:34:55.1'")).matches("INTERVAL '1.1' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.22' - TIME '12:34:55.11'")).matches("INTERVAL '1.11' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.222' - TIME '12:34:55.111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.2222' - TIME '12:34:55.1111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.22222' - TIME '12:34:55.11111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.222222' - TIME '12:34:55.111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.2222222' - TIME '12:34:55.1111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.22222222' - TIME '12:34:55.11111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.222222222' - TIME '12:34:55.111111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.2222222222' - TIME '12:34:55.1111111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.22222222222' - TIME '12:34:55.11111111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.222222222222' - TIME '12:34:55.111111111111'")).matches("INTERVAL '1.111' SECOND");

        // round up, positive
        assertThat(assertions.expression("TIME '12:34:56.9' - TIME '12:34:55.1'")).matches("INTERVAL '1.8' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.99' - TIME '12:34:55.11'")).matches("INTERVAL '1.88' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.999' - TIME '12:34:55.111'")).matches("INTERVAL '1.888' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.9999' - TIME '12:34:55.1111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.99999' - TIME '12:34:55.11111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.999999' - TIME '12:34:55.111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.9999999' - TIME '12:34:55.1111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.99999999' - TIME '12:34:55.11111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.999999999' - TIME '12:34:55.111111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.9999999999' - TIME '12:34:55.1111111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.99999999999' - TIME '12:34:55.11111111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.999999999999' - TIME '12:34:55.111111111111'")).matches("INTERVAL '1.889' SECOND");

        // round down, negative
        assertThat(assertions.expression("TIME '12:34:55' - TIME '12:34:56'")).matches("INTERVAL '-1' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.1' - TIME '12:34:56.2'")).matches("INTERVAL '-1.1' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.11' - TIME '12:34:56.22'")).matches("INTERVAL '-1.11' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.111' - TIME '12:34:56.222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.1111' - TIME '12:34:56.2222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.11111' - TIME '12:34:56.22222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.111111' - TIME '12:34:56.222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.1111111' - TIME '12:34:56.2222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.11111111' - TIME '12:34:56.22222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.111111111' - TIME '12:34:56.222222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.1111111111' - TIME '12:34:56.2222222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.11111111111' - TIME '12:34:56.22222222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.111111111111' - TIME '12:34:56.222222222222'")).matches("INTERVAL '-1.111' SECOND");

        // round up, negative
        assertThat(assertions.expression("TIME '12:34:55.1' - TIME '12:34:56.9'")).matches("INTERVAL '-1.8' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.11' - TIME '12:34:56.99'")).matches("INTERVAL '-1.88' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.111' - TIME '12:34:56.999'")).matches("INTERVAL '-1.888' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.1111' - TIME '12:34:56.9999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.11111' - TIME '12:34:56.99999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.111111' - TIME '12:34:56.999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.1111111' - TIME '12:34:56.9999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.11111111' - TIME '12:34:56.99999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.111111111' - TIME '12:34:56.999999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.1111111111' - TIME '12:34:56.9999999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.11111111111' - TIME '12:34:56.99999999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:55.111111111111' - TIME '12:34:56.999999999999'")).matches("INTERVAL '-1.889' SECOND");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as TIME)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "TIME '00:00:00'"))
                .isEqualTo(false);
    }
}
