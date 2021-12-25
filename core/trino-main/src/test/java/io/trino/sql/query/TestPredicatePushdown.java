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
public class TestPredicatePushdown
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
    public void testConditionalExpressionWithFailingExpression()
    {
        assertThat(assertions.query("" +
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
                "WHERE v = 'x' AND r IS NOT NULL"))
                .matches("VALUES ('x', 1)");

        assertThat(assertions.query(
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
                        "WHERE k = 1 AND r <> 0"))
                .matches("VALUES (1, 1)");

        assertThat(assertions.query(
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
                        "WHERE k = 1 AND r <> 0"))
                .matches("VALUES (1, 1)");
    }
}
