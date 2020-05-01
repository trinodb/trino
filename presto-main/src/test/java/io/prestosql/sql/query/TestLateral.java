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

public class TestLateral
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
    public void testUncorrelatedLateral()
    {
        assertions.assertQuery(
                "SELECT * FROM LATERAL (VALUES 1, 2, 3)",
                "VALUES 1, 2, 3");

        assertions.assertQuery(
                "SELECT * FROM LATERAL (VALUES 1), (VALUES 'a')",
                "VALUES (1, 'a')");

        assertions.assertQuery(
                "SELECT * FROM LATERAL (VALUES 1) CROSS JOIN (VALUES 'a')",
                "VALUES (1, 'a')");

        assertions.assertQuery(
                "SELECT * FROM LATERAL (VALUES 1) t(a)",
                "VALUES 1");

        // The nested LATERAL is uncorrelated with respect to the subquery it belongs to. The column comes
        // from the outer query
        assertions.assertQuery(
                "SELECT * FROM (VALUES 1) t(a), LATERAL (SELECT * FROM LATERAL (SELECT a))",
                "VALUES (1, 1)");

        assertions.assertQuery(
                "SELECT (SELECT * FROM LATERAL (SELECT a)) FROM (VALUES 1) t(a)",
                "VALUES 1");
    }

    @Test
    public void testNotInScope()
    {
        assertions.assertFails("SELECT * FROM (VALUES 1) t(a), (SELECT * FROM LATERAL (SELECT a))", "line 1:63: Column 'a' cannot be resolved");
    }
}
