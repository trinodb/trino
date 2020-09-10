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
package io.prestosql.operator.scalar.timestamptz;

import io.prestosql.Session;
import io.prestosql.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampWithTimeZoneToTimestampCast
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("Pacific/Apia"))
                .build();
        assertions = new QueryAssertions(session);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testCastToTimestamp()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.12'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.123'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123'");

        // source < target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.10'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.100'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.120'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234500'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234500000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234560'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345600'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234560000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345600000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345670'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456700'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345670000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456700000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456780'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567800'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456780000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567890'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678900'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678910'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789100'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789120'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555555556'");
    }
}
