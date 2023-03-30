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
package io.trino.operator.scalar.timestamp;

import io.trino.Session;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(TestingSession.DEFAULT_TIME_ZONE_KEY)
                .build();
        assertions = new QueryAssertions(session);
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
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' = TIMESTAMP '2020-05-01 12:34:56'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' = TIMESTAMP '2020-05-01 12:34:56.1'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' = TIMESTAMP '2020-05-01 12:34:56.12'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' = TIMESTAMP '2020-05-01 12:34:56.123'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' = TIMESTAMP '2020-05-01 12:34:56.1234'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' = TIMESTAMP '2020-05-01 12:34:56.12345'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' = TIMESTAMP '2020-05-01 12:34:56.123456'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' = TIMESTAMP '2020-05-01 12:34:56.1234567'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' = TIMESTAMP '2020-05-01 12:34:56.12345678'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' = TIMESTAMP '2020-05-01 12:34:56.123456789'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' = TIMESTAMP '2020-05-01 12:34:56.1234567890'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' = TIMESTAMP '2020-05-01 12:34:56.12345678901'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' = TIMESTAMP '2020-05-01 12:34:56.123456789012'")).isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' <> TIMESTAMP '2020-05-01 12:34:56'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' <> TIMESTAMP '2020-05-01 12:34:56.1'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' <> TIMESTAMP '2020-05-01 12:34:56.12'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' <> TIMESTAMP '2020-05-01 12:34:56.123'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' <> TIMESTAMP '2020-05-01 12:34:56.1234'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' <> TIMESTAMP '2020-05-01 12:34:56.12345'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' <> TIMESTAMP '2020-05-01 12:34:56.123456'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' <> TIMESTAMP '2020-05-01 12:34:56.1234567'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' <> TIMESTAMP '2020-05-01 12:34:56.12345678'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' <> TIMESTAMP '2020-05-01 12:34:56.123456789'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' <> TIMESTAMP '2020-05-01 12:34:56.1234567890'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' <> TIMESTAMP '2020-05-01 12:34:56.12345678901'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' <> TIMESTAMP '2020-05-01 12:34:56.123456789012'")).isEqualTo(false);

        // difference in the high-order data
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' <> TIMESTAMP '2021-05-01 12:34:56'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' <> TIMESTAMP '2021-05-01 12:34:56.1'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' <> TIMESTAMP '2021-05-01 12:34:56.12'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' <> TIMESTAMP '2021-05-01 12:34:56.123'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' <> TIMESTAMP '2021-05-01 12:34:56.1234'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' <> TIMESTAMP '2021-05-01 12:34:56.12345'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' <> TIMESTAMP '2021-05-01 12:34:56.123456'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' <> TIMESTAMP '2021-05-01 12:34:56.1234567'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' <> TIMESTAMP '2021-05-01 12:34:56.12345678'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' <> TIMESTAMP '2021-05-01 12:34:56.123456789'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' <> TIMESTAMP '2021-05-01 12:34:56.1234567890'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' <> TIMESTAMP '2021-05-01 12:34:56.12345678901'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' <> TIMESTAMP '2021-05-01 12:34:56.123456789012'")).isEqualTo(true);

        // difference in the low-order data
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' <> TIMESTAMP '2020-05-01 12:34:57'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' <> TIMESTAMP '2020-05-01 12:34:56.2'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' <> TIMESTAMP '2020-05-01 12:34:56.13'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' <> TIMESTAMP '2020-05-01 12:34:56.124'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' <> TIMESTAMP '2020-05-01 12:34:56.1235'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' <> TIMESTAMP '2020-05-01 12:34:56.12346'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' <> TIMESTAMP '2020-05-01 12:34:56.123457'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' <> TIMESTAMP '2020-05-01 12:34:56.1234568'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' <> TIMESTAMP '2020-05-01 12:34:56.12345679'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' <> TIMESTAMP '2020-05-01 12:34:56.123456781'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' <> TIMESTAMP '2020-05-01 12:34:56.1234567891'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' <> TIMESTAMP '2020-05-01 12:34:56.12345678902'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' <> TIMESTAMP '2020-05-01 12:34:56.123456789013'")).isEqualTo(true);
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567890'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678901'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789012'")).isEqualTo(false);

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:55'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.0'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.11'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.122'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1233'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12344'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123455'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234566'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345677'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456788'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567899'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678900'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789011'")).isEqualTo(true);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' < TIMESTAMP '2020-05-01 12:34:57'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' < TIMESTAMP '2020-05-01 12:34:56.2'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' < TIMESTAMP '2020-05-01 12:34:56.13'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' < TIMESTAMP '2020-05-01 12:34:56.124'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' < TIMESTAMP '2020-05-01 12:34:56.1235'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' < TIMESTAMP '2020-05-01 12:34:56.12346'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' < TIMESTAMP '2020-05-01 12:34:56.123457'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' < TIMESTAMP '2020-05-01 12:34:56.1234568'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' < TIMESTAMP '2020-05-01 12:34:56.12345679'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' < TIMESTAMP '2020-05-01 12:34:56.123456790'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' < TIMESTAMP '2020-05-01 12:34:56.1234567891'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' < TIMESTAMP '2020-05-01 12:34:56.12345678902'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' < TIMESTAMP '2020-05-01 12:34:56.123456789013'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' < TIMESTAMP '2020-05-01 12:34:56'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' < TIMESTAMP '2020-05-01 12:34:56.1'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' < TIMESTAMP '2020-05-01 12:34:56.12'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' < TIMESTAMP '2020-05-01 12:34:56.123'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' < TIMESTAMP '2020-05-01 12:34:56.1234'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' < TIMESTAMP '2020-05-01 12:34:56.12345'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' < TIMESTAMP '2020-05-01 12:34:56.123456'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' < TIMESTAMP '2020-05-01 12:34:56.1234567'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' < TIMESTAMP '2020-05-01 12:34:56.12345678'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' < TIMESTAMP '2020-05-01 12:34:56.123456789'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' < TIMESTAMP '2020-05-01 12:34:56.1234567890'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' < TIMESTAMP '2020-05-01 12:34:56.12345678901'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' < TIMESTAMP '2020-05-01 12:34:56.123456789012'")).isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' > TIMESTAMP '2020-05-01 12:34:55'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' > TIMESTAMP '2020-05-01 12:34:55.9'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' > TIMESTAMP '2020-05-01 12:34:56.11'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' > TIMESTAMP '2020-05-01 12:34:56.122'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' > TIMESTAMP '2020-05-01 12:34:56.1233'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' > TIMESTAMP '2020-05-01 12:34:56.12344'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' > TIMESTAMP '2020-05-01 12:34:56.123455'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' > TIMESTAMP '2020-05-01 12:34:56.1234566'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' > TIMESTAMP '2020-05-01 12:34:56.12345677'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' > TIMESTAMP '2020-05-01 12:34:56.123456788'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' > TIMESTAMP '2020-05-01 12:34:56.1234567889'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' > TIMESTAMP '2020-05-01 12:34:56.12345678899'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' > TIMESTAMP '2020-05-01 12:34:56.123456789011'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' > TIMESTAMP '2020-05-01 12:34:56'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' > TIMESTAMP '2020-05-01 12:34:56.1'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' > TIMESTAMP '2020-05-01 12:34:56.12'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' > TIMESTAMP '2020-05-01 12:34:56.123'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' > TIMESTAMP '2020-05-01 12:34:56.1234'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' > TIMESTAMP '2020-05-01 12:34:56.12345'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' > TIMESTAMP '2020-05-01 12:34:56.123456'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' > TIMESTAMP '2020-05-01 12:34:56.1234567'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' > TIMESTAMP '2020-05-01 12:34:56.12345678'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' > TIMESTAMP '2020-05-01 12:34:56.123456789'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' > TIMESTAMP '2020-05-01 12:34:56.1234567890'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' > TIMESTAMP '2020-05-01 12:34:56.12345678901'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' > TIMESTAMP '2020-05-01 12:34:56.123456789012'")).isEqualTo(false);
    }

    @Test
    public void testLessThanOrEquals()
    {
        // equality
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' <= TIMESTAMP '2020-05-01 12:34:56'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' <= TIMESTAMP '2020-05-01 12:34:56.1'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' <= TIMESTAMP '2020-05-01 12:34:56.12'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' <= TIMESTAMP '2020-05-01 12:34:56.123'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' <= TIMESTAMP '2020-05-01 12:34:56.1234'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' <= TIMESTAMP '2020-05-01 12:34:56.12345'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' <= TIMESTAMP '2020-05-01 12:34:56.123456'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' <= TIMESTAMP '2020-05-01 12:34:56.1234567'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' <= TIMESTAMP '2020-05-01 12:34:56.12345678'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' <= TIMESTAMP '2020-05-01 12:34:56.123456789'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' <= TIMESTAMP '2020-05-01 12:34:56.1234567890'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' <= TIMESTAMP '2020-05-01 12:34:56.12345678901'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' <= TIMESTAMP '2020-05-01 12:34:56.123456789012'")).isEqualTo(true);

        // less than
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' <= TIMESTAMP '2020-05-01 12:34:57'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' <= TIMESTAMP '2020-05-01 12:34:56.2'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' <= TIMESTAMP '2020-05-01 12:34:56.13'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' <= TIMESTAMP '2020-05-01 12:34:56.124'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' <= TIMESTAMP '2020-05-01 12:34:56.1235'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' <= TIMESTAMP '2020-05-01 12:34:56.12346'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' <= TIMESTAMP '2020-05-01 12:34:56.123457'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' <= TIMESTAMP '2020-05-01 12:34:56.1234568'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' <= TIMESTAMP '2020-05-01 12:34:56.12345679'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' <= TIMESTAMP '2020-05-01 12:34:56.123456790'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' <= TIMESTAMP '2020-05-01 12:34:56.1234567891'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' <= TIMESTAMP '2020-05-01 12:34:56.12345678902'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' <= TIMESTAMP '2020-05-01 12:34:56.123456789013'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' <= TIMESTAMP '2020-05-01 12:34:55'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' <= TIMESTAMP '2020-05-01 12:34:56.0'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' <= TIMESTAMP '2020-05-01 12:34:56.11'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' <= TIMESTAMP '2020-05-01 12:34:56.122'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' <= TIMESTAMP '2020-05-01 12:34:56.1233'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' <= TIMESTAMP '2020-05-01 12:34:56.12344'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' <= TIMESTAMP '2020-05-01 12:34:56.123454'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' <= TIMESTAMP '2020-05-01 12:34:56.1234566'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' <= TIMESTAMP '2020-05-01 12:34:56.12345677'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' <= TIMESTAMP '2020-05-01 12:34:56.123456788'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' <= TIMESTAMP '2020-05-01 12:34:56.1234567889'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' <= TIMESTAMP '2020-05-01 12:34:56.12345678900'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' <= TIMESTAMP '2020-05-01 12:34:56.123456789011'")).isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEquals()
    {
        // equality
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' >= TIMESTAMP '2020-05-01 12:34:56'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' >= TIMESTAMP '2020-05-01 12:34:56.1'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' >= TIMESTAMP '2020-05-01 12:34:56.12'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' >= TIMESTAMP '2020-05-01 12:34:56.123'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' >= TIMESTAMP '2020-05-01 12:34:56.1234'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' >= TIMESTAMP '2020-05-01 12:34:56.12345'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' >= TIMESTAMP '2020-05-01 12:34:56.123456'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' >= TIMESTAMP '2020-05-01 12:34:56.1234567'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' >= TIMESTAMP '2020-05-01 12:34:56.12345678'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' >= TIMESTAMP '2020-05-01 12:34:56.123456789'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' >= TIMESTAMP '2020-05-01 12:34:56.1234567890'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' >= TIMESTAMP '2020-05-01 12:34:56.12345678901'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' >= TIMESTAMP '2020-05-01 12:34:56.123456789012'")).isEqualTo(true);

        // greater than
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' >= TIMESTAMP '2020-05-01 12:34:55'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' >= TIMESTAMP '2020-05-01 12:34:55.9'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' >= TIMESTAMP '2020-05-01 12:34:56.11'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' >= TIMESTAMP '2020-05-01 12:34:56.122'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' >= TIMESTAMP '2020-05-01 12:34:56.1233'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' >= TIMESTAMP '2020-05-01 12:34:56.12344'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' >= TIMESTAMP '2020-05-01 12:34:56.123455'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' >= TIMESTAMP '2020-05-01 12:34:56.1234566'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' >= TIMESTAMP '2020-05-01 12:34:56.12345677'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' >= TIMESTAMP '2020-05-01 12:34:56.123456788'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' >= TIMESTAMP '2020-05-01 12:34:56.1234567889'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' >= TIMESTAMP '2020-05-01 12:34:56.12345678899'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' >= TIMESTAMP '2020-05-01 12:34:56.123456789011'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' >= TIMESTAMP '2020-05-01 12:34:57'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' >= TIMESTAMP '2020-05-01 12:34:56.2'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' >= TIMESTAMP '2020-05-01 12:34:56.13'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' >= TIMESTAMP '2020-05-01 12:34:56.124'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' >= TIMESTAMP '2020-05-01 12:34:56.1235'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' >= TIMESTAMP '2020-05-01 12:34:56.12346'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' >= TIMESTAMP '2020-05-01 12:34:56.123457'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' >= TIMESTAMP '2020-05-01 12:34:56.1234568'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' >= TIMESTAMP '2020-05-01 12:34:56.12345679'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' >= TIMESTAMP '2020-05-01 12:34:56.1234567891'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' >= TIMESTAMP '2020-05-01 12:34:56.1234567891'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' >= TIMESTAMP '2020-05-01 12:34:56.12345678902'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' >= TIMESTAMP '2020-05-01 12:34:56.123456789013'")).isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' BETWEEN TIMESTAMP '2020-05-01 12:34:55' and TIMESTAMP '2020-05-01 12:34:57'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' BETWEEN TIMESTAMP '2020-05-01 12:34:56.0' and TIMESTAMP '2020-05-01 12:34:56.2'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' BETWEEN TIMESTAMP '2020-05-01 12:34:56.11' and TIMESTAMP '2020-05-01 12:34:56.13'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' BETWEEN TIMESTAMP '2020-05-01 12:34:56.122' and TIMESTAMP '2020-05-01 12:34:56.124'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1233' and TIMESTAMP '2020-05-01 12:34:56.1235'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12344' and TIMESTAMP '2020-05-01 12:34:56.12346'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123455' and TIMESTAMP '2020-05-01 12:34:56.123457'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234566' and TIMESTAMP '2020-05-01 12:34:56.1234568'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12345677' and TIMESTAMP '2020-05-01 12:34:56.12345679'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456788' and TIMESTAMP '2020-05-01 12:34:56.123456790'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567889' and TIMESTAMP '2020-05-01 12:34:56.1234567891'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567890' and TIMESTAMP '2020-05-01 12:34:56.12345678902'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456789011' and TIMESTAMP '2020-05-01 12:34:56.123456789013'")).isEqualTo(true);
    }

    @Test
    public void testAddIntervalDayToSecond()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.123'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.223'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.243'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.246'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.2464'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.24645'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.246456'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.2464567'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.24645678'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.246456789'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.2464567890'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.24645678901'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.246456789012'");

        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56'")).matches("TIMESTAMP '2020-05-01 12:34:57.123'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1'")).matches("TIMESTAMP '2020-05-01 12:34:57.223'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12'")).matches("TIMESTAMP '2020-05-01 12:34:57.243'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123'")).matches("TIMESTAMP '2020-05-01 12:34:57.246'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234'")).matches("TIMESTAMP '2020-05-01 12:34:57.2464'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345'")).matches("TIMESTAMP '2020-05-01 12:34:57.24645'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456'")).matches("TIMESTAMP '2020-05-01 12:34:57.246456'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567'")).matches("TIMESTAMP '2020-05-01 12:34:57.2464567'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678'")).matches("TIMESTAMP '2020-05-01 12:34:57.24645678'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789'")).matches("TIMESTAMP '2020-05-01 12:34:57.246456789'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567890'")).matches("TIMESTAMP '2020-05-01 12:34:57.2464567890'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678901'")).matches("TIMESTAMP '2020-05-01 12:34:57.24645678901'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789012'")).matches("TIMESTAMP '2020-05-01 12:34:57.246456789012'");

        // interval is currently p = 3, so the timestamp(p) + interval day to second yields timestamp(3) for p = [0, 1, 2, 3]
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.000'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.100'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.120'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.123'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.1234'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.12345'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.123456'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.1234567'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.12345678'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.123456789'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.1234567890'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.12345678901'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.123456789012'");

        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56'")).matches("TIMESTAMP '2020-05-02 12:34:56.000'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1'")).matches("TIMESTAMP '2020-05-02 12:34:56.100'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12'")).matches("TIMESTAMP '2020-05-02 12:34:56.120'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123'")).matches("TIMESTAMP '2020-05-02 12:34:56.123'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234'")).matches("TIMESTAMP '2020-05-02 12:34:56.1234'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345'")).matches("TIMESTAMP '2020-05-02 12:34:56.12345'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456'")).matches("TIMESTAMP '2020-05-02 12:34:56.123456'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567'")).matches("TIMESTAMP '2020-05-02 12:34:56.1234567'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678'")).matches("TIMESTAMP '2020-05-02 12:34:56.12345678'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789'")).matches("TIMESTAMP '2020-05-02 12:34:56.123456789'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567890'")).matches("TIMESTAMP '2020-05-02 12:34:56.1234567890'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678901'")).matches("TIMESTAMP '2020-05-02 12:34:56.12345678901'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789012'")).matches("TIMESTAMP '2020-05-02 12:34:56.123456789012'");
    }

    @Test
    public void testSubtractIntervalDayToSecond()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:54.877'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:54.977'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:54.997'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.000'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.0004'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.00045'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.000456'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.0004567'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.00045678'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.000456789'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.0004567890'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.00045678901'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.000456789012'");

        // interval is currently p = 3, so the timestamp(p) - interval day to second yields timestamp(3) for p = [0, 1, 2, 3]
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.000'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.100'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.120'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.123'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.1234'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.12345'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.123456'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.1234567'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.12345678'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.123456789'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.1234567890'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.12345678901'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.123456789012'");
    }

    @Test
    public void testAddIntervalYearToMonth()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.1'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.12'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.123'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.1234'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.12345'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.123456'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.1234567'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.12345678'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.123456789'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.1234567890'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.12345678901'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.123456789012'");

        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56'")).matches("TIMESTAMP '2020-06-01 12:34:56'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1'")).matches("TIMESTAMP '2020-06-01 12:34:56.1'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12'")).matches("TIMESTAMP '2020-06-01 12:34:56.12'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123'")).matches("TIMESTAMP '2020-06-01 12:34:56.123'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234'")).matches("TIMESTAMP '2020-06-01 12:34:56.1234'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345'")).matches("TIMESTAMP '2020-06-01 12:34:56.12345'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456'")).matches("TIMESTAMP '2020-06-01 12:34:56.123456'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567'")).matches("TIMESTAMP '2020-06-01 12:34:56.1234567'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678'")).matches("TIMESTAMP '2020-06-01 12:34:56.12345678'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789'")).matches("TIMESTAMP '2020-06-01 12:34:56.123456789'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567890'")).matches("TIMESTAMP '2020-06-01 12:34:56.1234567890'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678901'")).matches("TIMESTAMP '2020-06-01 12:34:56.12345678901'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789012'")).matches("TIMESTAMP '2020-06-01 12:34:56.123456789012'");
    }

    @Test
    public void testSubtractIntervalYearToMonth()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.1'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.12'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.123'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.1234'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.12345'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.123456'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.1234567'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.12345678'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.123456789'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.1234567890'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.12345678901'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.123456789012'");
    }

    @Test
    public void testSubtract()
    {
        // round down, positive
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' - TIMESTAMP '2020-05-01 12:34:55'")).matches("INTERVAL '1' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.2' - TIMESTAMP '2020-05-01 12:34:55.1'")).matches("INTERVAL '1.1' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.22' - TIMESTAMP '2020-05-01 12:34:55.11'")).matches("INTERVAL '1.11' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.222' - TIMESTAMP '2020-05-01 12:34:55.111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.2222' - TIMESTAMP '2020-05-01 12:34:55.1111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.22222' - TIMESTAMP '2020-05-01 12:34:55.11111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.222222' - TIMESTAMP '2020-05-01 12:34:55.111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.2222222' - TIMESTAMP '2020-05-01 12:34:55.1111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.22222222' - TIMESTAMP '2020-05-01 12:34:55.11111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.222222222' - TIMESTAMP '2020-05-01 12:34:55.111111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.2222222222' - TIMESTAMP '2020-05-01 12:34:55.1111111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.22222222222' - TIMESTAMP '2020-05-01 12:34:55.11111111111'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.222222222222' - TIMESTAMP '2020-05-01 12:34:55.111111111111'")).matches("INTERVAL '1.111' SECOND");

        // round up, positive
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.9' - TIMESTAMP '2020-05-01 12:34:55.1'")).matches("INTERVAL '1.8' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.99' - TIMESTAMP '2020-05-01 12:34:55.11'")).matches("INTERVAL '1.88' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.999' - TIMESTAMP '2020-05-01 12:34:55.111'")).matches("INTERVAL '1.888' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.9999' - TIMESTAMP '2020-05-01 12:34:55.1111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.99999' - TIMESTAMP '2020-05-01 12:34:55.11111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.999999' - TIMESTAMP '2020-05-01 12:34:55.111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.9999999' - TIMESTAMP '2020-05-01 12:34:55.1111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.99999999' - TIMESTAMP '2020-05-01 12:34:55.11111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.999999999' - TIMESTAMP '2020-05-01 12:34:55.111111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.9999999999' - TIMESTAMP '2020-05-01 12:34:55.1111111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.99999999999' - TIMESTAMP '2020-05-01 12:34:55.11111111111'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.999999999999' - TIMESTAMP '2020-05-01 12:34:55.111111111111'")).matches("INTERVAL '1.889' SECOND");

        // round down, negative
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55' - TIMESTAMP '2020-05-01 12:34:56'")).matches("INTERVAL '-1' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.1' - TIMESTAMP '2020-05-01 12:34:56.2'")).matches("INTERVAL '-1.1' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.11' - TIMESTAMP '2020-05-01 12:34:56.22'")).matches("INTERVAL '-1.11' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.111' - TIMESTAMP '2020-05-01 12:34:56.222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.1111' - TIMESTAMP '2020-05-01 12:34:56.2222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.11111' - TIMESTAMP '2020-05-01 12:34:56.22222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.111111' - TIMESTAMP '2020-05-01 12:34:56.222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.1111111' - TIMESTAMP '2020-05-01 12:34:56.2222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.11111111' - TIMESTAMP '2020-05-01 12:34:56.22222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.111111111' - TIMESTAMP '2020-05-01 12:34:56.222222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.1111111111' - TIMESTAMP '2020-05-01 12:34:56.2222222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.11111111111' - TIMESTAMP '2020-05-01 12:34:56.22222222222'")).matches("INTERVAL '-1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.111111111111' - TIMESTAMP '2020-05-01 12:34:56.222222222222'")).matches("INTERVAL '-1.111' SECOND");

        // round up, negative
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.1' - TIMESTAMP '2020-05-01 12:34:56.9'")).matches("INTERVAL '-1.8' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.11' - TIMESTAMP '2020-05-01 12:34:56.99'")).matches("INTERVAL '-1.88' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.111' - TIMESTAMP '2020-05-01 12:34:56.999'")).matches("INTERVAL '-1.888' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.1111' - TIMESTAMP '2020-05-01 12:34:56.9999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.11111' - TIMESTAMP '2020-05-01 12:34:56.99999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.111111' - TIMESTAMP '2020-05-01 12:34:56.999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.1111111' - TIMESTAMP '2020-05-01 12:34:56.9999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.11111111' - TIMESTAMP '2020-05-01 12:34:56.99999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.111111111' - TIMESTAMP '2020-05-01 12:34:56.999999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.1111111111' - TIMESTAMP '2020-05-01 12:34:56.9999999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.11111111111' - TIMESTAMP '2020-05-01 12:34:56.99999999999'")).matches("INTERVAL '-1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:55.111111111111' - TIMESTAMP '2020-05-01 12:34:56.999999999999'")).matches("INTERVAL '-1.889' SECOND");
    }
}
