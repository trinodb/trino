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
package io.trino.operator.scalar.timetz;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
        assertThat(assertions.expression("TIME '12:34:56+08:35' = TIME '12:34:56+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1+08:35' = TIME '12:34:56.1+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12+08:35' = TIME '12:34:56.12+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123+08:35' = TIME '12:34:56.123+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234+08:35' = TIME '12:34:56.1234+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345+08:35' = TIME '12:34:56.12345+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456+08:35' = TIME '12:34:56.123456+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35' = TIME '12:34:56.1234567+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35' = TIME '12:34:56.12345678+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35' = TIME '12:34:56.123456789+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567891+08:35' = TIME '12:34:56.1234567891+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678912+08:35' = TIME '12:34:56.12345678912+08:35'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789123+08:35' = TIME '12:34:56.123456789123+08:35'")).isEqualTo(true);

        // TIME W/ TZ are equal if ( X - Y ) INTERVAL SECOND = INTERVAL '0' SECOND, which is performed modulo 24h
        assertThat(assertions.expression("TIME '00:00:00+14:00' = TIME '00:00:00-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.1+14:00' = TIME '00:00:00.1-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.12+14:00' = TIME '00:00:00.12-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.123+14:00' = TIME '00:00:00.123-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.1234+14:00' = TIME '00:00:00.1234-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.12345+14:00' = TIME '00:00:00.12345-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.123456+14:00' = TIME '00:00:00.123456-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.1234567+14:00' = TIME '00:00:00.1234567-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.12345678+14:00' = TIME '00:00:00.12345678-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.123456789+14:00' = TIME '00:00:00.123456789-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.1234567891+14:00' = TIME '00:00:00.1234567891-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.12345678912+14:00' = TIME '00:00:00.12345678912-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '00:00:00.123456789123+14:00' = TIME '00:00:00.123456789123-10:00'")).isEqualTo(true);

        assertThat(assertions.expression("TIME '12:34:56+08:35' = TIME '00:00:00+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1+08:35' = TIME '00:00:00.1+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12+08:35' = TIME '00:00:00.12+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123+08:35' = TIME '00:00:00.123+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234+08:35' = TIME '00:00:00.1234+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345+08:35' = TIME '00:00:00.12345+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456+08:35' = TIME '00:00:00.123456+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35' = TIME '00:00:00.1234567+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35' = TIME '00:00:00.12345678+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35' = TIME '00:00:00.123456789+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567891+08:35' = TIME '00:00:00.1234567891+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678912+08:35' = TIME '00:00:00.12345678912+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789123+08:35' = TIME '00:00:00.123456789123+08:35'")).isEqualTo(false);

        assertThat(assertions.expression("TIME '00:00:00+14:00' = TIME '00:00:00-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1+14:00' = TIME '00:00:00.1-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12+14:00' = TIME '00:00:00.12-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123+14:00' = TIME '00:00:00.123-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234+14:00' = TIME '00:00:00.1234-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345+14:00' = TIME '00:00:00.12345-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456+14:00' = TIME '00:00:00.123456-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234567+14:00' = TIME '00:00:00.1234567-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345678+14:00' = TIME '00:00:00.12345678-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456789+14:00' = TIME '00:00:00.123456789-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234567891+14:00' = TIME '00:00:00.1234567891-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345678912+14:00' = TIME '00:00:00.12345678912-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456789123+14:00' = TIME '00:00:00.123456789123-01:00'")).isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("TIME '12:34:56+08:35' <> TIME '12:34:56+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1+08:35' <> TIME '12:34:56.1+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12+08:35' <> TIME '12:34:56.12+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123+08:35' <> TIME '12:34:56.123+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234+08:35' <> TIME '12:34:56.1234+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345+08:35' <> TIME '12:34:56.12345+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456+08:35' <> TIME '12:34:56.123456+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35' <> TIME '12:34:56.1234567+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35' <> TIME '12:34:56.12345678+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35' <> TIME '12:34:56.123456789+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567891+08:35' <> TIME '12:34:56.1234567891+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678912+08:35' <> TIME '12:34:56.12345678912+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789123+08:35' <> TIME '12:34:56.123456789123+08:35'")).isEqualTo(false);

        // TIME W/ TZ are equal if ( X - Y ) INTERVAL SECOND <> INTERVAL '0' SECOND, which is performed modulo 24h
        assertThat(assertions.expression("TIME '00:00:00+14:00' <> TIME '00:00:00-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1+14:00' <> TIME '00:00:00.1-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12+14:00' <> TIME '00:00:00.12-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123+14:00' <> TIME '00:00:00.123-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234+14:00' <> TIME '00:00:00.1234-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345+14:00' <> TIME '00:00:00.12345-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456+14:00' <> TIME '00:00:00.123456-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234567+14:00' <> TIME '00:00:00.1234567-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345678+14:00' <> TIME '00:00:00.12345678-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456789+14:00' <> TIME '00:00:00.123456789-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234567891+14:00' <> TIME '00:00:00.1234567891-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345678912+14:00' <> TIME '00:00:00.12345678912-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456789123+14:00' <> TIME '00:00:00.123456789123-10:00'")).isEqualTo(false);

        assertThat(assertions.expression("TIME '12:34:56+08:35' <> TIME '12:34:56-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1+08:35' <> TIME '12:34:56.1-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12+08:35' <> TIME '12:34:56.12-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123+08:35' <> TIME '12:34:56.123-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234+08:35' <> TIME '12:34:56.1234-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345+08:35' <> TIME '12:34:56.12345-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456+08:35' <> TIME '12:34:56.123456-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35' <> TIME '12:34:56.1234567-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35' <> TIME '12:34:56.12345678-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35' <> TIME '12:34:56.123456789-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567891+08:35' <> TIME '12:34:56.1234567891-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678912+08:35' <> TIME '12:34:56.12345678912-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789123+08:35' <> TIME '12:34:56.123456789123-07:21'")).isEqualTo(true);
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.expression("TIME '12:34:56+08:35' IS DISTINCT FROM TIME '12:34:56+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1+08:35' IS DISTINCT FROM TIME '12:34:56.1+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12+08:35' IS DISTINCT FROM TIME '12:34:56.12+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123+08:35' IS DISTINCT FROM TIME '12:34:56.123+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234+08:35' IS DISTINCT FROM TIME '12:34:56.1234+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345+08:35' IS DISTINCT FROM TIME '12:34:56.12345+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456+08:35' IS DISTINCT FROM TIME '12:34:56.123456+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35' IS DISTINCT FROM TIME '12:34:56.1234567+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35' IS DISTINCT FROM TIME '12:34:56.12345678+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35' IS DISTINCT FROM TIME '12:34:56.123456789+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.1234567891+08:35' IS DISTINCT FROM TIME '12:34:56.1234567891+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.12345678912+08:35' IS DISTINCT FROM TIME '12:34:56.12345678912+08:35'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '12:34:56.123456789123+08:35' IS DISTINCT FROM TIME '12:34:56.123456789123+08:35'")).isEqualTo(false);

        // TIME W/ TZ are equal if ( X - Y ) INTERVAL SECOND IS DISTINCT FROM INTERVAL '0' SECOND, which is performed modulo 24h
        assertThat(assertions.expression("TIME '00:00:00+14:00' IS DISTINCT FROM TIME '00:00:00-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1+14:00' IS DISTINCT FROM TIME '00:00:00.1-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12+14:00' IS DISTINCT FROM TIME '00:00:00.12-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123+14:00' IS DISTINCT FROM TIME '00:00:00.123-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234+14:00' IS DISTINCT FROM TIME '00:00:00.1234-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345+14:00' IS DISTINCT FROM TIME '00:00:00.12345-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456+14:00' IS DISTINCT FROM TIME '00:00:00.123456-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234567+14:00' IS DISTINCT FROM TIME '00:00:00.1234567-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345678+14:00' IS DISTINCT FROM TIME '00:00:00.12345678-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456789+14:00' IS DISTINCT FROM TIME '00:00:00.123456789-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.1234567891+14:00' IS DISTINCT FROM TIME '00:00:00.1234567891-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.12345678912+14:00' IS DISTINCT FROM TIME '00:00:00.12345678912-10:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '00:00:00.123456789123+14:00' IS DISTINCT FROM TIME '00:00:00.123456789123-10:00'")).isEqualTo(false);

        assertThat(assertions.expression("TIME '12:34:56+08:35' IS DISTINCT FROM TIME '12:34:56-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1+08:35' IS DISTINCT FROM TIME '12:34:56.1-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12+08:35' IS DISTINCT FROM TIME '12:34:56.12-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123+08:35' IS DISTINCT FROM TIME '12:34:56.123-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234+08:35' IS DISTINCT FROM TIME '12:34:56.1234-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345+08:35' IS DISTINCT FROM TIME '12:34:56.12345-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456+08:35' IS DISTINCT FROM TIME '12:34:56.123456-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35' IS DISTINCT FROM TIME '12:34:56.1234567-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35' IS DISTINCT FROM TIME '12:34:56.12345678-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35' IS DISTINCT FROM TIME '12:34:56.123456789-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.1234567891+08:35' IS DISTINCT FROM TIME '12:34:56.1234567891-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.12345678912+08:35' IS DISTINCT FROM TIME '12:34:56.12345678912-07:21'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '12:34:56.123456789123+08:35' IS DISTINCT FROM TIME '12:34:56.123456789123-07:21'")).isEqualTo(true);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.expression("TIME '21:00:00-01:00' < TIME '03:00:00+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.1-01:00' < TIME '03:00:00.1+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.12-01:00' < TIME '03:00:00.12+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.123-01:00' < TIME '03:00:00.123+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.1234-01:00' < TIME '03:00:00.1234+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.12345-01:00' < TIME '03:00:00.12345+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.123456-01:00' < TIME '03:00:00.123456+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.1234567-01:00' < TIME '03:00:00.1234567+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.12345678-01:00' < TIME '03:00:00.12345678+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.123456789-01:00' < TIME '03:00:00.123456789+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.1234567891-01:00' < TIME '03:00:00.1234567891+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.12345678912-01:00' < TIME '03:00:00.12345678912+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.123456789123-01:00' < TIME '03:00:00.123456789123+01:00'")).isEqualTo(false);

        assertThat(assertions.expression("TIME '03:00:00+01:00' < TIME '21:00:00-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1+01:00' < TIME '21:00:00.1-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12+01:00' < TIME '21:00:00.12-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123+01:00' < TIME '21:00:00.123-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234+01:00' < TIME '21:00:00.1234-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345+01:00' < TIME '21:00:00.12345-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456+01:00' < TIME '21:00:00.123456-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234567+01:00' < TIME '21:00:00.1234567-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345678+01:00' < TIME '21:00:00.12345678-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456789+01:00' < TIME '21:00:00.123456789-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234567891+01:00' < TIME '21:00:00.1234567891-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345678912+01:00' < TIME '21:00:00.12345678912-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456789123+01:00' < TIME '21:00:00.123456789123-01:00'")).isEqualTo(true);

        // normalization is done modulo 24h
        assertThat(assertions.expression("TIME '03:00:00+01:00' < TIME '21:00:00-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1+01:00' < TIME '21:00:00.1-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12+01:00' < TIME '21:00:00.12-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123+01:00' < TIME '21:00:00.123-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234+01:00' < TIME '21:00:00.1234-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345+01:00' < TIME '21:00:00.12345-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456+01:00' < TIME '21:00:00.123456-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234567+01:00' < TIME '21:00:00.1234567-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345678+01:00' < TIME '21:00:00.12345678-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456789+01:00' < TIME '21:00:00.123456789-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234567891+01:00' < TIME '21:00:00.1234567891-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345678912+01:00' < TIME '21:00:00.12345678912-10:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456789123+01:00' < TIME '21:00:00.123456789123-10:00'")).isEqualTo(true);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("TIME '03:00:00+01:00' > TIME '21:00:00-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.1+01:00' > TIME '21:00:00.1-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.12+01:00' > TIME '21:00:00.12-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.123+01:00' > TIME '21:00:00.123-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.1234+01:00' > TIME '21:00:00.1234-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.12345+01:00' > TIME '21:00:00.12345-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.123456+01:00' > TIME '21:00:00.123456-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.1234567+01:00' > TIME '21:00:00.1234567-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.12345678+01:00' > TIME '21:00:00.12345678-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.123456789+01:00' > TIME '21:00:00.123456789-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.1234567891+01:00' > TIME '21:00:00.1234567891-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.12345678912+01:00' > TIME '21:00:00.12345678912-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.123456789123+01:00' > TIME '21:00:00.123456789123-01:00'")).isEqualTo(false);

        assertThat(assertions.expression("TIME '03:00:00-14:00' > TIME '09:00:00-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1-14:00' > TIME '09:00:00.1-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12-14:00' > TIME '09:00:00.12-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123-14:00' > TIME '09:00:00.123-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234-14:00' > TIME '09:00:00.1234-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345-14:00' > TIME '09:00:00.12345-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456-14:00' > TIME '09:00:00.123456-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234567-14:00' > TIME '09:00:00.1234567-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345678-14:00' > TIME '09:00:00.12345678-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456789-14:00' > TIME '09:00:00.123456789-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.1234567891-14:00' > TIME '09:00:00.1234567891-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.12345678912-14:00' > TIME '09:00:00.12345678912-03:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:00:00.123456789123-14:00' > TIME '09:00:00.123456789123-03:00'")).isEqualTo(true);

        // normalization is done modulo 24h
        assertThat(assertions.expression("TIME '21:00:00-10:00' > TIME '03:00:00+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.1-10:00' > TIME '03:00:00.1+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.12-10:00' > TIME '03:00:00.12+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.123-10:00' > TIME '03:00:00.123+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.1234-10:00' > TIME '03:00:00.1234+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.12345-10:00' > TIME '03:00:00.12345+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.123456-10:00' > TIME '03:00:00.123456+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.1234567-10:00' > TIME '03:00:00.1234567+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.12345678-10:00' > TIME '03:00:00.12345678+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.123456789-10:00' > TIME '03:00:00.123456789+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.1234567891-10:00' > TIME '03:00:00.1234567891+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.12345678912-10:00' > TIME '03:00:00.12345678912+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.123456789123-10:00' > TIME '03:00:00.123456789123+01:00'")).isEqualTo(true);
    }

    @Test
    public void testLessThanOrEquals()
    {
        assertThat(assertions.expression("TIME '21:00:00-01:00' <= TIME '03:00:00+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.1-01:00' <= TIME '03:00:00.1+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.12-01:00' <= TIME '03:00:00.12+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.123-01:00' <= TIME '03:00:00.123+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.1234-01:00' <= TIME '03:00:00.1234+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.12345-01:00' <= TIME '03:00:00.12345+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.123456-01:00' <= TIME '03:00:00.123456+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.1234567-01:00' <= TIME '03:00:00.1234567+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.12345678-01:00' <= TIME '03:00:00.12345678+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.123456789-01:00' <= TIME '03:00:00.123456789+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.1234567891-01:00' <= TIME '03:00:00.1234567891+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.12345678912-01:00' <= TIME '03:00:00.12345678912+01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '21:00:00.123456789123-01:00' <= TIME '03:00:00.123456789123+01:00'")).isEqualTo(false);

        assertThat(assertions.expression("TIME '21:00:00-01:00' <= TIME '23:00:00+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.1-01:00' <= TIME '23:00:00.1+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.12-01:00' <= TIME '23:00:00.12+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.123-01:00' <= TIME '23:00:00.123+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.1234-01:00' <= TIME '23:00:00.1234+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.12345-01:00' <= TIME '23:00:00.12345+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.123456-01:00' <= TIME '23:00:00.123456+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.1234567-01:00' <= TIME '23:00:00.1234567+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.12345678-01:00' <= TIME '23:00:00.12345678+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.123456789-01:00' <= TIME '23:00:00.123456789+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.1234567891-01:00' <= TIME '23:00:00.1234567891+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.12345678912-01:00' <= TIME '23:00:00.12345678912+01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '21:00:00.123456789123-01:00' <= TIME '23:00:00.123456789123+01:00'")).isEqualTo(true);
    }

    @Test
    public void testGreaterThanOrEquals()
    {
        assertThat(assertions.expression("TIME '03:00:00+01:00' >= TIME '21:00:00-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.1+01:00' >= TIME '21:00:00.1-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.12+01:00' >= TIME '21:00:00.12-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.123+01:00' >= TIME '21:00:00.123-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.1234+01:00' >= TIME '21:00:00.1234-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.12345+01:00' >= TIME '21:00:00.12345-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.123456+01:00' >= TIME '21:00:00.123456-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.1234567+01:00' >= TIME '21:00:00.1234567-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.12345678+01:00' >= TIME '21:00:00.12345678-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.123456789+01:00' >= TIME '21:00:00.123456789-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.1234567891+01:00' >= TIME '21:00:00.1234567891-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.12345678912+01:00' >= TIME '21:00:00.12345678912-01:00'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:00:00.123456789123+01:00' >= TIME '21:00:00.123456789123-01:00'")).isEqualTo(false);

        assertThat(assertions.expression("TIME '23:00:00+01:00' >= TIME '21:00:00-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.1+01:00' >= TIME '21:00:00.1-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.12+01:00' >= TIME '21:00:00.12-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.123+01:00' >= TIME '21:00:00.123-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.1234+01:00' >= TIME '21:00:00.1234-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.12345+01:00' >= TIME '21:00:00.12345-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.123456+01:00' >= TIME '21:00:00.123456-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.1234567+01:00' >= TIME '21:00:00.1234567-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.12345678+01:00' >= TIME '21:00:00.12345678-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.123456789+01:00' >= TIME '21:00:00.123456789-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.1234567891+01:00' >= TIME '21:00:00.1234567891-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.12345678912+01:00' >= TIME '21:00:00.12345678912-01:00'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '23:00:00.123456789123+01:00' >= TIME '21:00:00.123456789123-01:00'")).isEqualTo(true);
    }

    @Test
    public void testAddIntervalDayToSecond()
    {
        assertThat(assertions.expression("TIME '12:34:56+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.123+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.223+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.243+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.246+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.2464+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.24645+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.246456+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.2464567+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.24645678+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.246456789+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234567890+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.2464567890+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345678901+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.24645678901+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456789012+08:35' + INTERVAL '1.123' SECOND")).matches("TIME '12:34:57.246456789012+08:35'");

        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56+08:35'")).matches("TIME '12:34:57.123+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.1+08:35'")).matches("TIME '12:34:57.223+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.12+08:35'")).matches("TIME '12:34:57.243+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.123+08:35'")).matches("TIME '12:34:57.246+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.1234+08:35'")).matches("TIME '12:34:57.2464+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.12345+08:35'")).matches("TIME '12:34:57.24645+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.123456+08:35'")).matches("TIME '12:34:57.246456+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.1234567+08:35'")).matches("TIME '12:34:57.2464567+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.12345678+08:35'")).matches("TIME '12:34:57.24645678+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.123456789+08:35'")).matches("TIME '12:34:57.246456789+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.1234567890+08:35'")).matches("TIME '12:34:57.2464567890+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.12345678901+08:35'")).matches("TIME '12:34:57.24645678901+08:35'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIME '12:34:56.123456789012+08:35'")).matches("TIME '12:34:57.246456789012+08:35'");

        // carry
        assertThat(assertions.expression("TIME '12:59:59+08:35' + INTERVAL '1' SECOND")).matches("TIME '13:00:00.000+08:35'");
        assertThat(assertions.expression("TIME '12:59:59.999+08:35' + INTERVAL '0.001' SECOND")).matches("TIME '13:00:00.000+08:35'");

        // wrap-around
        assertThat(assertions.expression("TIME '12:34:56+08:35' + INTERVAL '13' HOUR")).matches("TIME '01:34:56.000+08:35'");
    }

    @Test
    public void testSubtractIntervalDayToSecond()
    {
        assertThat(assertions.expression("TIME '12:34:56+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:54.877+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:54.977+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:54.997+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.000+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.0004+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.00045+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.000456+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.0004567+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.00045678+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.000456789+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234567890+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.0004567890+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345678901+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.00045678901+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456789012+08:35' - INTERVAL '1.123' SECOND")).matches("TIME '12:34:55.000456789012+08:35'");

        // borrow
        assertThat(assertions.expression("TIME '13:00:00+08:35' - INTERVAL '1' SECOND")).matches("TIME '12:59:59.000+08:35'");
        assertThat(assertions.expression("TIME '13:00:00+08:35' - INTERVAL '0.001' SECOND")).matches("TIME '12:59:59.999+08:35'");

        // wrap-around
        assertThat(assertions.expression("TIME '12:34:56+08:35' - INTERVAL '13' HOUR")).matches("TIME '23:34:56.000+08:35'");
    }

    @Test
    public void testSubtract()
    {
        // round down
        assertThat(assertions.expression("TIME '12:34:56+08:35' - TIME '12:34:55+08:35'")).matches("INTERVAL '1' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.2+08:35' - TIME '12:34:55.1+08:35'")).matches("INTERVAL '1.1' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.22+08:35' - TIME '12:34:55.11+08:35'")).matches("INTERVAL '1.11' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.222+08:35' - TIME '12:34:55.111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.2222+08:35' - TIME '12:34:55.1111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.22222+08:35' - TIME '12:34:55.11111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.222222+08:35' - TIME '12:34:55.111111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.2222222+08:35' - TIME '12:34:55.1111111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.22222222+08:35' - TIME '12:34:55.11111111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.222222222+08:35' - TIME '12:34:55.111111111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.2222222222+08:35' - TIME '12:34:55.1111111111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.22222222222+08:35' - TIME '12:34:55.11111111111+08:35'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.222222222222+08:35' - TIME '12:34:55.111111111111+08:35'")).matches("INTERVAL '1.111' SECOND");

        // round up
        assertThat(assertions.expression("TIME '12:34:56.9+08:35' - TIME '12:34:55.1+08:35'")).matches("INTERVAL '1.8' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.99+08:35' - TIME '12:34:55.11+08:35'")).matches("INTERVAL '1.88' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.999+08:35' - TIME '12:34:55.111+08:35'")).matches("INTERVAL '1.888' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.9999+08:35' - TIME '12:34:55.1111+08:35'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.99999+08:35' - TIME '12:34:55.11111+08:35'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.999999+08:35' - TIME '12:34:55.111111+08:35'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.9999999+08:35' - TIME '12:34:55.1111111+08:35'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.99999999+08:35' - TIME '12:34:55.11111111+08:35'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.999999999+08:35' - TIME '12:34:55.111111111+08:35'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.9999999999+08:35' - TIME '12:34:55.1111111111+08:35'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.99999999999+08:35' - TIME '12:34:55.11111111111+08:35'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIME '12:34:56.999999999999+08:35' - TIME '12:34:55.111111111111+08:35'")).matches("INTERVAL '1.889' SECOND");

        // different timezone, positive result
        assertThat(assertions.expression("TIME '09:00:00-14:00' - TIME '19:00:00+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.0-14:00' - TIME '19:00:00.0+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.00-14:00' - TIME '19:00:00.00+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.000-14:00' - TIME '19:00:00.000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.0000-14:00' - TIME '19:00:00.0000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.00000-14:00' - TIME '19:00:00.00000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.000000-14:00' - TIME '19:00:00.000000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.0000000-14:00' - TIME '19:00:00.0000000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.00000000-14:00' - TIME '19:00:00.00000000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.000000000-14:00' - TIME '19:00:00.000000000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.0000000000-14:00' - TIME '19:00:00.0000000000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.00000000000-14:00' - TIME '19:00:00.00000000000+03:00'")).matches("INTERVAL '7' HOUR");
        assertThat(assertions.expression("TIME '09:00:00.000000000000-14:00' - TIME '19:00:00.000000000000+03:00'")).matches("INTERVAL '7' HOUR");

        // different timezone, negative result
        assertThat(assertions.expression("TIME '19:00:00+03:00' - TIME '09:00:00-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.0+03:00' - TIME '09:00:00.0-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.00+03:00' - TIME '09:00:00.00-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.000+03:00' - TIME '09:00:00.000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.0000+03:00' - TIME '09:00:00.0000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.00000+03:00' - TIME '09:00:00.00000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.000000+03:00' - TIME '09:00:00.000000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.0000000+03:00' - TIME '09:00:00.0000000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.00000000+03:00' - TIME '09:00:00.00000000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.000000000+03:00' - TIME '09:00:00.000000000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.0000000000+03:00' - TIME '09:00:00.0000000000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.00000000000+03:00' - TIME '09:00:00.00000000000-14:00'")).matches("INTERVAL '-7' HOUR");
        assertThat(assertions.expression("TIME '19:00:00.000000000000+03:00' - TIME '09:00:00.000000000000-14:00'")).matches("INTERVAL '-7' HOUR");
    }
}
