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
package io.trino.operator.scalar;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestFormatNumberFunction
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
    public void testFormatNumber()
    {
        assertThat(assertions.function("format_number", "TINYINT '123'"))
                .hasType(VARCHAR)
                .isEqualTo("123");

        assertThat(assertions.function("format_number", "SMALLINT '12345'"))
                .hasType(VARCHAR)
                .isEqualTo("12.3K");

        assertThat(assertions.function("format_number", "SMALLINT '12399'"))
                .hasType(VARCHAR)
                .isEqualTo("12.4K");

        assertThat(assertions.function("format_number", "INTEGER '12345678'"))
                .hasType(VARCHAR)
                .isEqualTo("12.3M");

        assertThat(assertions.function("format_number", "INTEGER '12399999'"))
                .hasType(VARCHAR)
                .isEqualTo("12.4M");

        assertThat(assertions.function("format_number", "BIGINT '12345678901'"))
                .hasType(VARCHAR)
                .isEqualTo("12.3B");

        assertThat(assertions.function("format_number", "BIGINT '12399999999'"))
                .hasType(VARCHAR)
                .isEqualTo("12.4B");

        assertThat(assertions.function("format_number", "DOUBLE '1234.5'"))
                .hasType(VARCHAR)
                .isEqualTo("1.23K");

        assertThat(assertions.function("format_number", "DOUBLE '1239.9'"))
                .hasType(VARCHAR)
                .isEqualTo("1.24K");

        assertThat(assertions.function("format_number", "REAL '1234567.8'"))
                .hasType(VARCHAR)
                .isEqualTo("1.23M");

        assertThat(assertions.function("format_number", "REAL '1239999.9'"))
                .hasType(VARCHAR)
                .isEqualTo("1.24M");

        assertThat(assertions.function("format_number", "DECIMAL '1234567890.1'"))
                .hasType(VARCHAR)
                .isEqualTo("1.23B");

        assertThat(assertions.function("format_number", "DECIMAL '1239999999.9'"))
                .hasType(VARCHAR)
                .isEqualTo("1.24B");

        assertThat(assertions.function("format_number", "-999"))
                .hasType(VARCHAR)
                .isEqualTo("-999");

        assertThat(assertions.function("format_number", "-1000"))
                .hasType(VARCHAR)
                .isEqualTo("-1K");

        assertThat(assertions.function("format_number", "-999999"))
                .hasType(VARCHAR)
                .isEqualTo("-1000K");

        assertThat(assertions.function("format_number", "-1000000"))
                .hasType(VARCHAR)
                .isEqualTo("-1M");

        assertThat(assertions.function("format_number", "-999999999"))
                .hasType(VARCHAR)
                .isEqualTo("-1000M");

        assertThat(assertions.function("format_number", "-1000000000"))
                .hasType(VARCHAR)
                .isEqualTo("-1B");

        assertThat(assertions.function("format_number", "-999999999999"))
                .hasType(VARCHAR)
                .isEqualTo("-1000B");

        assertThat(assertions.function("format_number", "-1000000000000"))
                .hasType(VARCHAR)
                .isEqualTo("-1T");

        assertThat(assertions.function("format_number", "-999999999999999"))
                .hasType(VARCHAR)
                .isEqualTo("-1000T");

        assertThat(assertions.function("format_number", "-1000000000000000"))
                .hasType(VARCHAR)
                .isEqualTo("-1Q");

        assertThat(assertions.function("format_number", "-9223372036854775808"))
                .hasType(VARCHAR)
                .isEqualTo("-9223.37Q");

        assertThat(assertions.function("format_number", "0"))
                .hasType(VARCHAR)
                .isEqualTo("0");

        assertThat(assertions.function("format_number", "999"))
                .hasType(VARCHAR)
                .isEqualTo("999");

        assertThat(assertions.function("format_number", "1000"))
                .hasType(VARCHAR)
                .isEqualTo("1K");

        assertThat(assertions.function("format_number", "999999"))
                .hasType(VARCHAR)
                .isEqualTo("1000K");

        assertThat(assertions.function("format_number", "1000000"))
                .hasType(VARCHAR)
                .isEqualTo("1M");

        assertThat(assertions.function("format_number", "999999999"))
                .hasType(VARCHAR)
                .isEqualTo("1000M");

        assertThat(assertions.function("format_number", "1000000000"))
                .hasType(VARCHAR)
                .isEqualTo("1B");

        assertThat(assertions.function("format_number", "999999999999"))
                .hasType(VARCHAR)
                .isEqualTo("1000B");

        assertThat(assertions.function("format_number", "1000000000000"))
                .hasType(VARCHAR)
                .isEqualTo("1T");

        assertThat(assertions.function("format_number", "999999999999999"))
                .hasType(VARCHAR)
                .isEqualTo("1000T");

        assertThat(assertions.function("format_number", "1000000000000000"))
                .hasType(VARCHAR)
                .isEqualTo("1Q");

        assertThat(assertions.function("format_number", "9223372036854775807"))
                .hasType(VARCHAR)
                .isEqualTo("9223Q");

        assertThat(assertions.function("format_number", "CAST(NULL AS TINYINT)"))
                .isNull(VARCHAR);

        assertThat(assertions.function("format_number", "CAST(NULL AS SMALLINT)"))
                .isNull(VARCHAR);

        assertThat(assertions.function("format_number", "CAST(NULL AS INTEGER)"))
                .isNull(VARCHAR);

        assertThat(assertions.function("format_number", "CAST(NULL AS DOUBLE)"))
                .isNull(VARCHAR);

        assertThat(assertions.function("format_number", "CAST(NULL AS REAL)"))
                .isNull(VARCHAR);

        assertThat(assertions.function("format_number", "CAST(NULL AS DECIMAL)"))
                .isNull(VARCHAR);
    }
}
