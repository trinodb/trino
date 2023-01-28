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
package io.trino.operator.scalar.timestamptz;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOperators
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
    public void testEqual()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'")).isEqualTo(true);

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56 +04:44' = TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'")).isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).isEqualTo(false);

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56 +04:44' <> TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'")).isEqualTo(false);

        // difference in the high-order data
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567890 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678901 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789012 Asia/Kathmandu'")).isEqualTo(true);

        // difference in the low-order data
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456781 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'")).isEqualTo(true);
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).isEqualTo(false);

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'")).isEqualTo(false);

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567899 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678900 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'")).isEqualTo(true);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55.9 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678899 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).isEqualTo(false);
    }

    @Test
    public void testLessThanOrEquals()
    {
        // equality
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).isEqualTo(true);

        // less than
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123454 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678900 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'")).isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEquals()
    {
        // equality
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).isEqualTo(true);

        // greater than
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55.9 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678899 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'")).isEqualTo(true);

        // false cases
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'")).isEqualTo(false);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'")).isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'")).isEqualTo(true);
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'")).isEqualTo(true);
    }

    @Test
    public void testAddIntervalDayToSecond()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.123 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.223 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.243 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.246 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.2464 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.24645 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.246456 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.2464567 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.24645678 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.246456789 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.2464567890 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.24645678901 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:57.246456789012 Asia/Kathmandu'");

        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.123 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.223 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.243 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.246 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.2464 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.24645 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.246456 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.2464567 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.24645678 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.246456789 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.2464567890 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.24645678901 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-01 12:34:57.246456789012 Asia/Kathmandu'");

        // interval is currently p = 3, so the timestamp(p) + interval day to second yields timestamp(3) for p = [0, 1, 2, 3]
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.000 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.100 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.120 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.123 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.1234 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.12345 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.123456 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.1234567 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.12345678 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.123456789 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.1234567890 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.12345678901 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1' DAY")).matches("TIMESTAMP '2020-05-02 12:34:56.123456789012 Asia/Kathmandu'");

        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.000 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.100 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.120 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.123 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.1234 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.12345 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.123456 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.1234567 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.12345678 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.123456789 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.1234567890 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.12345678901 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).matches("TIMESTAMP '2020-05-02 12:34:56.123456789012 Asia/Kathmandu'");
    }

    @Test
    public void testSubtractIntervalDayToSecond()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:54.877 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:54.977 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:54.997 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.000 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.0004 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.00045 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.000456 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.0004567 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.00045678 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.000456789 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.0004567890 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.00045678901 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1.123' SECOND")).matches("TIMESTAMP '2020-05-01 12:34:55.000456789012 Asia/Kathmandu'");

        // interval is currently p = 3, so the timestamp(p) - interval day to second yields timestamp(3) for p = [0, 1, 2, 3]
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.000 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.100 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.120 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.123 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.1234 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.12345 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.123456 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.1234567 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.12345678 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.123456789 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.1234567890 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.12345678901 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1' DAY")).matches("TIMESTAMP '2020-04-30 12:34:56.123456789012 Asia/Kathmandu'");
    }

    @Test
    public void testAddIntervalYearToMonth()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.12 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.123 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.1234 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.12345 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.123456 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.1234567 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.12345678 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.123456789 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.1234567890 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.12345678901 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-06-01 12:34:56.123456789012 Asia/Kathmandu'");

        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.12 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.123 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.1234 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.12345 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.123456 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.1234567 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.12345678 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.123456789 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.1234567890 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.12345678901 Asia/Kathmandu'");
        assertThat(assertions.expression("INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'")).matches("TIMESTAMP '2020-06-01 12:34:56.123456789012 Asia/Kathmandu'");

        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.1 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.1 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.12 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.12 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.123 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.123 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.1234 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.1234 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.12345 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.12345 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.123456 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.123456 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.1234567 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.1234567 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.12345678 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.12345678 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.123456789 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.123456789 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.1234567890 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.1234567890 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.12345678901 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.12345678901 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.123456789012 Asia/Kathmandu' + INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-11-01 01:02:03.123456789012 Asia/Kathmandu'");
    }

    @Test
    public void testSubtractIntervalYearToMonth()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.12 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.123 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.1234 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.12345 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.123456 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.1234567 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.12345678 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.123456789 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.1234567890 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.12345678901 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-04-01 12:34:56.123456789012 Asia/Kathmandu'");

        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.1 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.1 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.12 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.12 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.123 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.123 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.1234 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.1234 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.12345 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.12345 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.123456 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.123456 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.1234567 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.1234567 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.12345678 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.12345678 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.123456789 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.123456789 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.1234567890 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.1234567890 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.12345678901 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.12345678901 Asia/Kathmandu'");
        assertThat(assertions.expression("TIMESTAMP '2020-10-01 01:02:03.123456789012 Asia/Kathmandu' - INTERVAL '1' MONTH")).matches("TIMESTAMP '2020-09-01 01:02:03.123456789012 Asia/Kathmandu'");
    }

    @Test
    public void testSubtract()
    {
        // round down
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'")).matches("INTERVAL '1' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1 Asia/Kathmandu'")).matches("INTERVAL '1.1' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.22 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11 Asia/Kathmandu'")).matches("INTERVAL '1.11' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.2222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.22222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.2222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.22222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.2222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.22222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.222222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111 Asia/Kathmandu'")).matches("INTERVAL '1.111' SECOND");

        // round up
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.9 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1 Asia/Kathmandu'")).matches("INTERVAL '1.8' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.99 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11 Asia/Kathmandu'")).matches("INTERVAL '1.88' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111 Asia/Kathmandu'")).matches("INTERVAL '1.888' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.9999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.99999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.9999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.99999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.9999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.99999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.999999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111 Asia/Kathmandu'")).matches("INTERVAL '1.889' SECOND");

        // negative difference in sub-millisecond fraction, round up
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.0002 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.00022 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.000222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.0002222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.00022222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.000222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.0002222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555555 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.00022222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555555 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.000222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555555 Asia/Kathmandu'")).matches("INTERVAL '1.000' SECOND");

        // negative difference in sub-millisecond fraction, round down
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.0002 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.00022 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.000222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.0002222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.00022222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.000222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.0002222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999999 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.00022222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999999 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.000222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999999 Asia/Kathmandu'")).matches("INTERVAL '0.999' SECOND");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as TIMESTAMP WITH TIME ZONE)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "TIMESTAMP '2001-01-02 01:04:05.321 +02:09'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);
    }
}
