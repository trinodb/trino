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

public class TestJoin
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
    public void testCrossJoinEliminationWithOuterJoin()
    {
        assertions.assertQuery(
                "WITH " +
                        "  a AS (SELECT id FROM (VALUES (1)) AS t(id))," +
                        "  b AS (SELECT id FROM (VALUES (1)) AS t(id))," +
                        "  c AS (SELECT id FROM (VALUES ('1')) AS t(id))," +
                        "  d as (SELECT id FROM (VALUES (1)) AS t(id))" +
                        "SELECT a.id " +
                        "FROM a " +
                        "LEFT JOIN b ON a.id = b.id " +
                        "JOIN c ON a.id = CAST(c.id AS bigint) " +
                        "JOIN d ON d.id = a.id",
                "VALUES 1");
    }
}
