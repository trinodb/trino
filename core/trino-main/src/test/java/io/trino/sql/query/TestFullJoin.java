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
public class TestFullJoin
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
    public void testFullJoinWithLimit()
    {
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) AS l(v) FULL OUTER JOIN (VALUES 2, 1) AS r(v) ON l.v = r.v LIMIT 1"))
                .satisfies(actual -> assertThat(actual.getMaterializedRows())
                        .hasSize(1)
                        .containsAnyElementsOf(assertions.execute("VALUES (1,1), (2,2)").getMaterializedRows()));

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) AS l(v) FULL OUTER JOIN (VALUES 2) AS r(v) ON l.v = r.v " +
                        "ORDER BY l.v NULLS FIRST " +
                        "LIMIT 1"))
                .matches("VALUES (1, CAST(NULL AS INTEGER))");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 2) AS l(v) FULL OUTER JOIN (VALUES 1, 2) AS r(v) ON l.v = r.v " +
                        "ORDER BY r.v NULLS FIRST " +
                        "LIMIT 1"))
                .matches("VALUES (CAST(NULL AS INTEGER), 1)");
    }
}
