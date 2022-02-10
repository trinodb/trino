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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigInteger;

import static java.lang.String.format;
import static java.math.BigInteger.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestWindowFrameRows
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
    public void testOffsetTypes()
    {
        String expected = "VALUES " +
                "ARRAY[null, null, 1], " +
                "ARRAY[null, null, 1, 2], " +
                "ARRAY[null, 1, 2, 2], " +
                "ARRAY[1, 2, 2], " +
                "ARRAY[2, 2]";

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN TINYINT '1' PRECEDING AND TINYINT '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN SMALLINT '1' PRECEDING AND SMALLINT '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN INTEGER '1' PRECEDING AND INTEGER '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN BIGINT '1' PRECEDING AND BIGINT '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        // short decimal
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN DECIMAL '1' PRECEDING AND DECIMAL '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        expected = "VALUES " +
                "ARRAY[null, null, 1, 2, 2], " +
                "ARRAY[null, null, 1, 2, 2], " +
                "ARRAY[null, 1, 2, 2], " +
                "ARRAY[1, 2, 2], " +
                "ARRAY[2, 2]";

        // short decimal: no integer overflow exception when frame offset exceeds integer
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND DECIMAL '%d' FOLLOWING) " +
                        "FROM (VALUES 2, 2, 1, null, null) t(a)",
                1L + Integer.MAX_VALUE)))
                .matches(expected);

        // long decimal: value does not overflow long
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND DECIMAL '%d' FOLLOWING) " +
                        "FROM (VALUES 2, 2, 1, null, null) t(a)",
                Long.MAX_VALUE)))
                .matches(expected);

        // long decimal: value overflows long so it is truncated to max long
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND DECIMAL '%s' FOLLOWING) " +
                        "FROM (VALUES 2, 2, 1, null, null) t(a)",
                BigInteger.valueOf(Long.MAX_VALUE).add(ONE))))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND DECIMAL '999999999999999999999999999999' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);
    }
}
