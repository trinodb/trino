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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestWindowFrameRange
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
    public void testNullsSortKey()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[1, 1, 2, 2], " +
                        "ARRAY[1, 1, 2, 2], " +
                        "ARRAY[1, 1, 2, 2, 3], " +
                        "ARRAY[1, 1, 2, 2, 3], " +
                        "ARRAY[2, 2, 3]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1, 2, 2], " +
                        "ARRAY[1, 1, 2, 2], " +
                        "ARRAY[1, 1, 2, 2, 3], " +
                        "ARRAY[1, 1, 2, 2, 3], " +
                        "ARRAY[2, 2, 3], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[3, 2, 2], " +
                        "ARRAY[3, 2, 2, 1, 1], " +
                        "ARRAY[3, 2, 2, 1, 1], " +
                        "ARRAY[2, 2, 1, 1], " +
                        "ARRAY[2, 2, 1, 1]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[3, 2, 2], " +
                        "ARRAY[3, 2, 2, 1, 1], " +
                        "ARRAY[3, 2, 2, 1, 1], " +
                        "ARRAY[2, 2, 1, 1], " +
                        "ARRAY[2, 2, 1, 1], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 2, null, null], " +
                        "ARRAY[1, 2, null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 2], " +
                        "ARRAY[1, 2], " +
                        "ARRAY[1, 2, null, null], " +
                        "ARRAY[1, 2, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 1, 2], " +
                        "ARRAY[null, null, 1, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 2], " +
                        "ARRAY[null, null, 1, 2], " +
                        "ARRAY[1, 2], " +
                        "ARRAY[1, 2]");
    }

    @Test
    public void testNoValueFrameBounds()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 1, 1], " +
                        "ARRAY[null, null, 1, 1], " +
                        "ARRAY[null, null, 1, 1, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN CURRENT ROW AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[2]");
    }

    @Test
    public void testMixedTypeFrameBoundsAscendingNullsFirst()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 1, 1]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[null, null, 1, 1, 2], " +
                        "ARRAY[2], " +
                        "ARRAY[2], " +
                        "null");
    }

    @Test
    public void testMixedTypeFrameBoundsAscendingNullsLast()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "null, " +
                        "null, " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[1, 1, 2, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[1, 1, 2, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[2], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1, 2], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[1, 1, 2, null, null], " +
                        "ARRAY[2, null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 0.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[2, null, null], " +
                        "ARRAY[2, null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");
    }

    @Test
    public void testMixedTypeFrameBoundsDescendingNullsFirst()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 2], " +
                        "ARRAY[null, null, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 2], " +
                        "ARRAY[null, null, 2, 1, 1], " +
                        "ARRAY[null, null, 2, 1, 1]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[2, 1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[2], " +
                        "ARRAY[2, 1, 1], " +
                        "ARRAY[2, 1, 1]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 2, 1, 1], " +
                        "ARRAY[null, null, 2, 1, 1], " +
                        "ARRAY[2, 1, 1], " +
                        "ARRAY[2, 1, 1], " +
                        "ARRAY[2, 1, 1]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null, null, 2, 1, 1], " +
                        "ARRAY[null, null, 2, 1, 1], " +
                        "null, " +
                        "null, " +
                        "null");
    }

    @Test
    public void testMixedTypeFrameBoundsDescendingNullsLast()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "null, " +
                        "ARRAY[2], " +
                        "ARRAY[2], " +
                        "ARRAY[2, 1, 1, null, null], " +
                        "ARRAY[2, 1, 1, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[2, 1, 1], " +
                        "ARRAY[2, 1, 1], " +
                        "ARRAY[2, 1, 1], " +
                        "ARRAY[2, 1, 1, null, null], " +
                        "ARRAY[2, 1, 1, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[2, 1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND CURRENT ROW) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[2], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[2, 1, 1, null, null], " +
                        "ARRAY[1, 1, null, null], " +
                        "ARRAY[1, 1, null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES 1, null, null, 2, 1) t(a)"))
                .matches("VALUES " +
                        "CAST(ARRAY[null, null] AS array(integer)), " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");
    }

    @Test
    public void testEmptyInput()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (SELECT 1 WHERE false) t(a)"))
                .returnsEmptyResult();
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE UNBOUNDED PRECEDING) " +
                "FROM (SELECT 1 WHERE false) t(a)"))
                .returnsEmptyResult();
    }

    @Test
    public void testEmptyFrame()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 10 PRECEDING) " +
                "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 10 FOLLOWING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 0.5 FOLLOWING AND 1.5 FOLLOWING) " +
                "FROM (VALUES 1, 2, 4) t(a)"))
                .matches("VALUES " +
                        "ARRAY[2], " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM (VALUES 1.0, 1.1) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(decimal(2, 1))), " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a NULLS LAST RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM (VALUES 1.0, 1.1, null) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(decimal(2, 1))), " +
                        "null, " +
                        "ARRAY[null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES 1.0, 1.1) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(decimal(2, 1))), " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES null, 1.0, 1.1) t(a)"))
                .matches("VALUES " +
                        "CAST(ARRAY[null] AS array(decimal(2,1))), " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES 1, 2) t(a)"))
                .matches("VALUES " +
                        "null, " +
                        "ARRAY[1]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES null, 1, 2) t(a)"))
                .matches("VALUES " +
                        "ARRAY[null], " +
                        "null, " +
                        "ARRAY[1]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1.5 PRECEDING) " +
                "FROM (VALUES null, 1, 2) t(a)"))
                .matches("VALUES " +
                        "CAST(ARRAY[null] AS array(integer)), " +
                        "null, " +
                        "null");
    }

    @Test
    public void testOnlyNulls()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM (VALUES CAST(null AS integer), null, null) t(a)"))
                .matches("VALUES " +
                        "CAST(ARRAY[null, null, null] AS array(integer)), " +
                        "ARRAY[null, null, null], " +
                        "ARRAY[null, null, null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES CAST(null AS integer), null, null) t(a)"))
                .matches("VALUES " +
                        "CAST(ARRAY[null, null, null] AS array(integer)), " +
                        "ARRAY[null, null, null], " +
                        "ARRAY[null, null, null]");
    }

    @Test
    public void testAllPartitionSameValues()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM (VALUES 1, 1, 1) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM (VALUES 1, 1, 1) t(a)"))
                .matches("VALUES " +
                        "CAST(null AS array(integer)), " +
                        "null, " +
                        "null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES 1, 1, 1) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1, 1], " +
                        "ARRAY[1, 1, 1], " +
                        "ARRAY[1, 1, 1]");
    }

    @Test
    public void testZeroOffset()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING) " +
                "FROM (VALUES 1, 2, 1, null) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 1], " +
                        "ARRAY[1, 1], " +
                        "ARRAY[2], " +
                        "ARRAY[null]");
    }

    @Test
    public void testNonConstantOffset()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN x * 10 PRECEDING AND y / 10.0 FOLLOWING) " +
                "FROM (VALUES (1, 0.1, 10), (2, 0.2, 20), (4, 0.4, 40)) t(a, x, y)"))
                .matches("VALUES " +
                        "ARRAY[1, 2], " +
                        "ARRAY[1, 2, 4], " +
                        "ARRAY[1, 2, 4]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN x * 10 PRECEDING AND y / 10.0 FOLLOWING) " +
                "FROM (VALUES (1, 0.1, 10), (2, 0.2, 20), (4, 0.4, 40), (null, 0.5, 50)) t(a, x, y)"))
                .matches("VALUES " +
                        "ARRAY[1, 2], " +
                        "ARRAY[1, 2, 4], " +
                        "ARRAY[1, 2, 4], " +
                        "ARRAY[null]");
    }

    @Test
    public void testInvalidOffset()
    {
        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC RANGE x PRECEDING) " +
                "FROM (VALUES (1, 0.1), (2, -0.2)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 0.1), (2, -0.2)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE x PRECEDING) " +
                "FROM (VALUES (1, 0.1), (2, -0.2)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 0.1), (2, -0.2)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE x PRECEDING) " +
                "FROM (VALUES (1, 0.1), (2, null)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 0.1), (2, null)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        // fail if offset is invalid for null sort key
        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 0.1), (null, null)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                "FROM (VALUES (1, 0.1), (null, -0.1)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        // test invalid offset of different types
        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (1, BIGINT '-1')) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (1, INTEGER '-1')) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (SMALLINT '1', SMALLINT '-1')) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (TINYINT '1', TINYINT '-1')) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (1, -1.1e0)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (1, REAL '-1.1')) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (1, -1.0001)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' YEAR)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' MONTH)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' DAY)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' HOUR)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' MINUTE)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");

        assertThatThrownBy(() -> assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' SECOND)) t(a, x)"))
                .hasMessage("Window frame offset value must not be negative or null");
    }

    @Test
    public void testWindowPartitioning()
    {
        assertThat(assertions.query("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y')) t(a, p)"))
                .matches("VALUES " +
                        "(null, 'x', ARRAY[null]), " +
                        "(1,    'x', ARRAY[1, 2]), " +
                        "(2,    'x', ARRAY[2]), " +
                        "(null, 'y', ARRAY[null]), " +
                        "(2,    'y', ARRAY[2])");

        assertThat(assertions.query("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y'), (null, null), (null, null), (1, null)) t(a, p)"))
                .matches("VALUES " +
                        "(null, null, ARRAY[null, null]), " +
                        "(null, null, ARRAY[null, null]), " +
                        "(1,    null, ARRAY[1]), " +
                        "(null, 'x', ARRAY[null]), " +
                        "(1,    'x', ARRAY[1, 2]), " +
                        "(2,    'x', ARRAY[2]), " +
                        "(null, 'y', ARRAY[null]), " +
                        "(2,    'y', ARRAY[2])");
    }

    @Test
    public void testTypes()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN DOUBLE '0.5' PRECEDING AND TINYINT '1' FOLLOWING) " +
                "FROM (VALUES 1, null, 2) t(a)"))
                .matches("VALUES " +
                        "ARRAY[1, 2], " +
                        "ARRAY[2], " +
                        "ARRAY[null]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 0.5 PRECEDING AND 1.000 FOLLOWING) " +
                "FROM (VALUES REAL '1', null, 2) t(a)"))
                .matches("VALUES " +
                        "ARRAY[REAL '1', REAL '2'], " +
                        "ARRAY[REAL '2'], " +
                        "ARRAY[null]");

        assertThat(assertions.query("SELECT x, array_agg(x) OVER(ORDER BY x DESC RANGE BETWEEN interval '1' month PRECEDING AND interval '1' month FOLLOWING) " +
                "FROM (VALUES DATE '2001-01-31', DATE '2001-08-25', DATE '2001-09-25', DATE '2001-09-26') t(x)"))
                .matches("VALUES " +
                        "(DATE '2001-09-26', ARRAY[DATE '2001-09-26', DATE '2001-09-25']), " +
                        "(DATE '2001-09-25', ARRAY[DATE '2001-09-26', DATE '2001-09-25', DATE '2001-08-25']), " +
                        "(DATE '2001-08-25', ARRAY[DATE '2001-09-25', DATE '2001-08-25']), " +
                        "(DATE '2001-01-31', ARRAY[DATE '2001-01-31'])");

        // January 31 + 1 month sets the frame bound to the last day of February. March 1 is out of range.
        assertThat(assertions.query("SELECT x, array_agg(x) OVER(ORDER BY x RANGE BETWEEN CURRENT ROW AND interval '1' month FOLLOWING) " +
                "FROM (VALUES DATE '2001-01-31', DATE '2001-02-28', DATE '2001-03-01') t(x)"))
                .matches("VALUES " +
                        "(DATE '2001-01-31', ARRAY[DATE '2001-01-31', DATE '2001-02-28']), " +
                        "(DATE '2001-02-28', ARRAY[DATE '2001-02-28', DATE '2001-03-01']), " +
                        "(DATE '2001-03-01', ARRAY[DATE '2001-03-01'])");

        assertThat(assertions.query("SELECT x, array_agg(x) OVER(ORDER BY x RANGE BETWEEN interval '1' year PRECEDING AND interval '1' month FOLLOWING) " +
                "FROM (VALUES " +
                "INTERVAL '1' month, " +
                "INTERVAL '2' month, " +
                "INTERVAL '5' year) t(x)"))
                .matches("VALUES " +
                        "(INTERVAL '1' month, ARRAY[INTERVAL '1' month, INTERVAL '2' month]), " +
                        "(INTERVAL '2' month, ARRAY[INTERVAL '1' month, INTERVAL '2' month]), " +
                        "(INTERVAL '5' year, ARRAY[INTERVAL '5' year])");
    }

    @Test
    public void testMultipleWindowFunctions()
    {
        assertThat(assertions.query("SELECT x, array_agg(date) OVER(ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), avg(number) OVER(ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                "FROM (VALUES " +
                "(2, DATE '2222-01-01', 4.4), " +
                "(1, DATE '1111-01-01', 2.2), " +
                "(3, DATE '3333-01-01', 6.6)) t(x, date, number)"))
                .matches("VALUES " +
                        "(1, ARRAY[DATE '1111-01-01', DATE '2222-01-01'], 3.3), " +
                        "(2, ARRAY[DATE '1111-01-01', DATE '2222-01-01', DATE '3333-01-01'], 4.4), " +
                        "(3, ARRAY[DATE '2222-01-01', DATE '3333-01-01'], 5.5)");

        assertThat(assertions.query("SELECT x, array_agg(a) OVER(ORDER BY x RANGE BETWEEN 2 PRECEDING AND CURRENT ROW), array_agg(a) OVER(ORDER BY x RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) " +
                "FROM (VALUES " +
                "(1.0, 1), " +
                "(2.0, 2), " +
                "(3.0, 3), " +
                "(4.0, 4), " +
                "(5.0, 5), " +
                "(6.0, 6)) t(x, a)"))
                .matches("VALUES " +
                        "(1.0, ARRAY[1], ARRAY[1, 2, 3]), " +
                        "(2.0, ARRAY[1, 2], ARRAY[2, 3, 4]), " +
                        "(3.0, ARRAY[1, 2, 3], ARRAY[3, 4, 5]), " +
                        "(4.0, ARRAY[2, 3, 4], ARRAY[4, 5, 6]), " +
                        "(5.0, ARRAY[3, 4, 5], ARRAY[5, 6]), " +
                        "(6.0, ARRAY[4, 5, 6], ARRAY[6])");
    }
}
