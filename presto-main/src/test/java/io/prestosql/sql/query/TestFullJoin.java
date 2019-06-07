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

import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.sql.query.QueryAssertions.assertContains;
import static org.testng.Assert.assertEquals;

public class TestFullJoin
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
    public void testFullJoinWithLimit()
    {
        MaterializedResult actual = assertions.execute(
                "SELECT * FROM (VALUES 1, 2) AS l(v) FULL OUTER JOIN (VALUES 2, 1) AS r(v) ON l.v = r.v LIMIT 1");

        assertEquals(actual.getMaterializedRows().size(), 1);
        assertContains(assertions.execute("VALUES (1,1), (2,2)"), actual);

        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2) AS l(v) FULL OUTER JOIN (VALUES 2) AS r(v) ON l.v = r.v " +
                        "ORDER BY l.v NULLS FIRST " +
                        "LIMIT 1",
                "VALUES (1, CAST(NULL AS INTEGER))");

        assertions.assertQuery(
                "SELECT * FROM (VALUES 2) AS l(v) FULL OUTER JOIN (VALUES 1, 2) AS r(v) ON l.v = r.v " +
                        "ORDER BY r.v NULLS FIRST " +
                        "LIMIT 1",
                "VALUES (CAST(NULL AS INTEGER), 1)");

        assertEquals(actual.getMaterializedRows().size(), 1);
        assertContains(assertions.execute("VALUES (1,1), (2,2)"), actual);
    }
}
