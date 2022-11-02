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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestWindowFrameGroups
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
    public void testConstantOffset()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 2, 2], " +
                        "ARRAY[null, null, 1, 2, 2], " +
                        "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                        "ARRAY[1, 2, 2, 3, 3, 3], " +
                        "ARRAY[1, 2, 2, 3, 3, 3], " +
                        "ARRAY[2, 2, 3, 3, 3], " +
                        "ARRAY[2, 2, 3, 3, 3], " +
                        "ARRAY[2, 2, 3, 3, 3]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS CURRENT ROW) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[1], " +
                        "ARRAY[2, 2], " +
                        "ARRAY[2, 2], " +
                        "ARRAY[3, 3, 3], " +
                        "ARRAY[3, 3, 3], " +
                        "ARRAY[3, 3, 3]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 0 PRECEDING AND 0 FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[1], " +
                        "ARRAY[2, 2], " +
                        "ARRAY[2, 2], " +
                        "ARRAY[3, 3, 3], " +
                        "ARRAY[3, 3, 3], " +
                        "ARRAY[3, 3, 3]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 2, 2], " +
                        "ARRAY[1, 2, 2], " +
                        "ARRAY[2, 2, 3, 3, 3], " +
                        "ARRAY[3, 3, 3], " +
                        "ARRAY[3, 3, 3], " +
                        "null, " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "null, " +
                        "null, " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 1], " +
                        "ARRAY[null, null, 1], " +
                        "ARRAY[1, 2, 2], " +
                        "ARRAY[1, 2, 2], " +
                        "ARRAY[1, 2, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null");
    }

    @Test
    public void testOffsetTypes()
    {
        String expected = "VALUES " +
                "ARRAY[null, null, 1, 2, 2], " +
                "ARRAY[null, null, 1, 2, 2], " +
                "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                "ARRAY[1, 2, 2, 3, 3, 3], " +
                "ARRAY[1, 2, 2, 3, 3, 3], " +
                "ARRAY[2, 2, 3, 3, 3], " +
                "ARRAY[2, 2, 3, 3, 3], " +
                "ARRAY[2, 2, 3, 3, 3]";

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN TINYINT '1' PRECEDING AND TINYINT '2' FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN SMALLINT '1' PRECEDING AND SMALLINT '2' FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN INTEGER '1' PRECEDING AND INTEGER '2' FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN BIGINT '1' PRECEDING AND BIGINT '2' FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        // short decimal
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN DECIMAL '1' PRECEDING AND DECIMAL '2' FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        expected = "VALUES " +
                "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                "ARRAY[1, 2, 2, 3, 3, 3], " +
                "ARRAY[1, 2, 2, 3, 3, 3], " +
                "ARRAY[2, 2, 3, 3, 3], " +
                "ARRAY[2, 2, 3, 3, 3], " +
                "ARRAY[2, 2, 3, 3, 3]";

        // short decimal: no integer overflow exception when frame offset exceeds integer
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND DECIMAL '%d' FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)",
                1L + Integer.MAX_VALUE)))
                .matches(expected);

        // long decimal: value does not overflow long
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND DECIMAL '%d' FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)",
                Long.MAX_VALUE)))
                .matches(expected);

        // long decimal: value overflows long so it is truncated to max long
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND DECIMAL '%s' FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)",
                BigInteger.valueOf(Long.MAX_VALUE).add(ONE))))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND DECIMAL '999999999999999999999999999999' FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches(expected);
    }

    @Test
    public void testNoValueFrameBounds()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 1, 1], " +
                        "ARRAY[null, null, 1, 1], " +
                        "ARRAY[null, null, 1, 1, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[2]");
    }

    @Test
    public void testMixedTypeFrameBounds()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "null, " +
                        "null, " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[1, 1, 2, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[2, null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[2, null, null], " +
                        "ARRAY[2, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[2, null, null], " +
                        "ARRAY[2, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[2, null, null], " +
                        "ARRAY[2, null, null], " +
                        "ARRAY[null, null], " +
                        "null, " +
                        "null");
    }

    @Test
    public void testEmptyFrame()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 90 PRECEDING AND 100 PRECEDING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 100 FOLLOWING AND 90 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null");
    }

    @Test
    public void testNonConstantOffset()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x PRECEDING AND y FOLLOWING) " +
                "FROM (VALUES ('a', 1, 1), ('b', 2, 0), ('c', 0, 3)) t(a, x, y)"))
                .matches("VALUES " +
                        "ARRAY['a', 'b'], " +
                        "ARRAY['a', 'b'], " +
                        "ARRAY['c']");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x FOLLOWING AND y FOLLOWING) " +
                "FROM (VALUES ('a', 1, 1), ('b', 2, 0), ('c', 3, 3), ('d', 0, 0)) t(a, x, y)"))
                .matches("VALUES " +
                        "ARRAY['b'], " +
                        "null, " +
                        "null, " +
                        "ARRAY['d']");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x PRECEDING AND y PRECEDING) " +
                "FROM (VALUES ('a', 1, 1), ('b', 0, 2), ('c', 2, 1), ('d', 0, 2)) t(a, x, y)"))
                .matches("VALUES " +
                        "null, " +
                        "null, " +
                        "ARRAY['a', 'b'], " +
                        "null");
    }

    @Test
    public void testEmptyInput()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (SELECT 1 WHERE false) t(a)"))
                .returnsEmptyResult();
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS UNBOUNDED PRECEDING) " +
                "FROM (SELECT 1 WHERE false) t(a)"))
                .returnsEmptyResult();
    }

    @Test
    public void testOnlyNulls()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) " +
                "FROM (VALUES CAST(null AS integer), null, null) t(a)"))
                .matches("VALUES " +
                        "CAST(ARRAY[null, null, null] AS array(integer)), " +
                        "ARRAY[null, null, null], " +
                        "ARRAY[null, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM (VALUES CAST(null AS integer), null, null) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES CAST(null AS integer), null, null) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null");
    }

    @Test
    public void testAllPartitionSameValues()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM (VALUES 'a', 'a', 'a') t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(varchar(1))), " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES 'a', 'a', 'a') t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(varchar(1))), " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 'a', 'a', 'a') t(a)"))
                .matches("VALUES " +
                        "ARRAY['a', 'a', 'a'], " +
                        "ARRAY['a', 'a', 'a'], " +
                        "ARRAY['a', 'a', 'a']");

        // test frame bounds at partition bounds
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 10 PRECEDING AND 10 FOLLOWING) " +
                "FROM (VALUES 'a', 'a', 'a') t(a)"))
                .matches("VALUES " +
                        "ARRAY['a', 'a', 'a'], " +
                        "ARRAY['a', 'a', 'a'], " +
                        "ARRAY['a', 'a', 'a']");
    }

    @Test
    public void testInvalidOffset()
    {
        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC GROUPS x PRECEDING) " +
                "FROM (VALUES (1, 1), (2, -2)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 1), (2, -2)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS x PRECEDING) " +
                "FROM (VALUES (1, 1), (2, -2)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 1), (2, -2)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS x PRECEDING) " +
                "FROM (VALUES (1, 1), (2, null)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 1), (2, null)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        // fail if offset is invalid for null sort key
        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 1), (null, null)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 1), (null, -1)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        // test invalid offset of different types
        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS x PRECEDING) " +
                "FROM (VALUES (1, BIGINT '-1')) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a GROUPS x PRECEDING) " +
                "FROM (VALUES (1, INTEGER '-1')) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");
    }

    @Test
    public void testWindowPartitioning()
    {
        assertThat(assertions.query("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y')) t(a, p)"))
                .matches("VALUES " +
                        "(null, 'x', ARRAY[null, 1]), " +
                        "(1,    'x', ARRAY[null, 1, 2]), " +
                        "(2,    'x', ARRAY[1, 2]), " +
                        "(null, 'y', ARRAY[null, 2]), " +
                        "(2,    'y', ARRAY[null, 2])");

        assertThat(assertions.query("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 0 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y'), (null, null), (null, null), (1, null)) t(a, p)"))
                .matches("VALUES " +
                        "(null, null, ARRAY[null, null, 1]), " +
                        "(null, null, ARRAY[null, null, 1]), " +
                        "(1,    null, ARRAY[1]), " +
                        "(null, 'x', ARRAY[null, 1]), " +
                        "(1,    'x', ARRAY[1, 2]), " +
                        "(2,    'x', ARRAY[2]), " +
                        "(null, 'y', ARRAY[null, 2]), " +
                        "(2,    'y', ARRAY[2])");
    }

    @Test
    public void testMultipleWindowFunctions()
    {
        // two functions with frame type GROUPS
        assertThat(assertions.query("SELECT x, array_agg(date) OVER(ORDER BY x GROUPS BETWEEN 1 PRECEDING AND 1 PRECEDING), avg(number) OVER(ORDER BY x GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) " +
                "FROM (VALUES " +
                "(2, DATE '2222-01-01', 4.4), " +
                "(1, DATE '1111-01-01', 2.2), " +
                "(3, DATE '3333-01-01', 6.6)) t(x, date, number)"))
                .matches("VALUES " +
                        "(1, null, 4.4), " +
                        "(2, ARRAY[DATE '1111-01-01'], 6.6), " +
                        "(3, ARRAY[DATE '2222-01-01'], null)");

        // three functions with different frame types
        assertThat(assertions.query("SELECT " +
                "x, " +
                "array_agg(a) OVER(ORDER BY x RANGE BETWEEN 2 PRECEDING AND CURRENT ROW), " +
                "array_agg(a) OVER(ORDER BY x GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING), " +
                "array_agg(a) OVER(ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                "FROM (VALUES " +
                "(1.0, 1), " +
                "(2.0, 2), " +
                "(3.0, 3), " +
                "(4.0, 4), " +
                "(5.0, 5), " +
                "(6.0, 6)) t(x, a)"))
                .matches("VALUES " +
                        "(1.0, ARRAY[1], ARRAY[2, 3], ARRAY[1]), " +
                        "(2.0, ARRAY[1, 2], ARRAY[3, 4], ARRAY[1, 2]), " +
                        "(3.0, ARRAY[1, 2, 3], ARRAY[4, 5], ARRAY[2, 3]), " +
                        "(4.0, ARRAY[2, 3, 4], ARRAY[5, 6], ARRAY[3, 4]), " +
                        "(5.0, ARRAY[3, 4, 5], ARRAY[6], ARRAY[4, 5]), " +
                        "(6.0, ARRAY[4, 5, 6], null, ARRAY[5, 6])");
    }

    @Test
    public void testOffsetOverflowsInteger()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 0 PRECEDING AND 1234567890123456789 FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                        "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                        "ARRAY[1, 2, 2, 3, 3, 3], " +
                        "ARRAY[2, 2, 3, 3, 3], " +
                        "ARRAY[2, 2, 3, 3, 3], " +
                        "ARRAY[3, 3, 3], " +
                        "ARRAY[3, 3, 3], " +
                        "ARRAY[3, 3, 3]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1234567890123456789 PRECEDING AND 0 FOLLOWING) " +
                "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 1], " +
                        "ARRAY[null, null, 1, 2, 2], " +
                        "ARRAY[null, null, 1, 2, 2], " +
                        "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                        "ARRAY[null, null, 1, 2, 2, 3, 3, 3], " +
                        "ARRAY[null, null, 1, 2, 2, 3, 3, 3]");
    }
}
