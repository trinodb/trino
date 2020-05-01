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
package io.prestosql.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPredicatePushdown
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testConditionalExpressionWithFailingExpression()
    {
        assertions.assertQuery("" +
                        "WITH t (k, a) AS ( " +
                        "    VALUES " +
                        "        (1, 1), " +
                        "        (2, 0) " +
                        ")," +
                        "u (k, v) AS ( " +
                        "    VALUES" +
                        "        (1, 'x'), " +
                        "        (2, 'y') " +
                        ") " +
                        "SELECT * " +
                        "FROM ( " +
                        "    SELECT v, if(v = 'x', 1 / a) AS r" +
                        "    FROM t JOIN u ON t.k = u.k " +
                        ") " +
                        "WHERE v = 'x' AND r IS NOT NULL",
                "VALUES ('x', 1)");

        assertions.assertQuery(
                "WITH t (k, v) AS ( " +
                        "    VALUES " +
                        "        (1, 1), " +
                        "        (2, 0) " +
                        "), " +
                        "u (k) AS ( " +
                        "    VALUES 1, 2 " +
                        ") " +
                        "SELECT * " +
                        "FROM ( " +
                        "    SELECT t.k, if(t.k = 1, 1 / t.v) AS r " +
                        "    FROM t JOIN u ON t.k = u.k " +
                        ") " +
                        "WHERE k = 1 AND r <> 0",
                "VALUES (1, 1)");

        assertions.assertQuery(
                "WITH t (k, v) AS ( " +
                        "    VALUES " +
                        "        (1, 1), " +
                        "        (2, 0) " +
                        "), " +
                        "u (k) AS ( " +
                        "    VALUES 1, 2 " +
                        ") " +
                        "SELECT * " +
                        "FROM ( " +
                        "    SELECT t.k, if(t.k = 1, 1 / t.v) AS r " +
                        "    FROM t LEFT JOIN u ON t.k = u.k " +
                        ") " +
                        "WHERE k = 1 AND r <> 0",
                "VALUES (1, 1)");
    }
}
