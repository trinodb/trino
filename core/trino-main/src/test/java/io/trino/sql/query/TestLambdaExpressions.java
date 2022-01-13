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
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLambdaExpressions
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
    public void testDuplicateLambdaExpressions()
    {
        assertThat(assertions.query("" +
                "SELECT cardinality(filter(a, x -> x > 0)) " +
                "FROM (VALUES " +
                "   ARRAY[1,2,3], " +
                "   ARRAY[0,1,2]," +
                "   ARRAY[0,0,0]" +
                ") AS t(a) " +
                "GROUP BY cardinality(filter(a, x -> x > 0))" +
                "ORDER BY cardinality(filter(a, x -> x > 0))"))
                .matches("VALUES BIGINT '0', BIGINT '2', BIGINT '3'");

        // same type
        assertThat(assertions.query("" +
                "SELECT transform(a, x -> x + 1), transform(b, x -> x + 1) " +
                "FROM (VALUES ROW(ARRAY[1, 2, 3], ARRAY[10, 20, 30])) t(a, b)"))
                .matches("VALUES ROW(ARRAY[2, 3, 4], ARRAY[11, 21, 31])");

        // different type
        assertThat(assertions.query("" +
                "SELECT transform(a, x -> x + 1), transform(b, x -> x + 1) " +
                "FROM (VALUES ROW(ARRAY[1, 2, 3], ARRAY[10e0, 20e0, 30e0])) t(a, b)"))
                .matches("VALUES ROW(ARRAY[2, 3, 4], ARRAY[11e0, 21e0, 31e0])");
    }

    @Test
    public void testParameterName()
    {
        // lambda may be using parameter which is not valid identifier in Java
        assertThat(assertions.query("" +
                "WITH t AS ( " +
                "    SELECT count(*) AS \"a.b c; d\" FROM (VALUES (42)) " +
                "    UNION ALL " +
                "    SELECT * FROM (VALUES (77)) v(\"a.b c; d\") " +
                ") " +
                "SELECT transform(ARRAY[1], x -> x + \"a.b c; d\") FROM t"))
                .matches("VALUES ARRAY[BIGINT '2'], ARRAY[BIGINT '78']");
    }

    @Test
    public void testNestedLambda()
    {
        // same argument name
        assertThat(assertions.query("" +
                "SELECT transform(a, x -> transform(ARRAY[x], x -> x + 1)) " +
                "FROM (VALUES ARRAY[1, 2, 3]) t(a)"))
                .matches("VALUES ARRAY[ARRAY[2], ARRAY[3], ARRAY[4]]");

        // different argument name
        assertThat(assertions.query("" +
                "SELECT transform(a, x -> transform(ARRAY[x], y -> y + 1)) " +
                "FROM (VALUES ARRAY[1, 2, 3]) t(a)"))
                .matches("VALUES ARRAY[ARRAY[2], ARRAY[3], ARRAY[4]]");
    }
}
