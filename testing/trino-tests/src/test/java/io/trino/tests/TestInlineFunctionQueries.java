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
package io.trino.tests;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

public class TestInlineFunctionQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testInlineFunction()
    {
        assertQuery(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    RETURN x * 2
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey = 1
                """,
                "VALUES(2)");

        assertQuery(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    RETURN x * 2
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey >= 1
                """,
                "SELECT nationkey * 2 FROM nation WHERE nationkey >= 1");

        assertQuery(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    RETURN x * 2
                SELECT my_func(nationkey)
                FROM nation
                """,
                "SELECT nationkey * 2 FROM nation");
    }

    @Test
    public void testInlineFunctionWithIf()
    {
        assertQuery(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    RETURN if(x=1, NULL, x)
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey = 1
                """,
                "VALUES(cast(NULL as bigint))");
    }

    @Test
    public void testInlineFunctionWithNullIf()
    {
        assertQuery(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    RETURN nullif(x, 1)
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey = 1
                """,
                "VALUES(cast(NULL as bigint))");
    }
}
