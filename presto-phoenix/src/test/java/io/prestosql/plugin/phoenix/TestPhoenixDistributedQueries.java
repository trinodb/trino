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
package io.prestosql.plugin.phoenix;

import io.prestosql.tests.AbstractTestDistributedQueries;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.phoenix.PhoenixQueryRunner.createPhoenixQueryRunner;

@Test
public class TestPhoenixDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestPhoenixDistributedQueries()
    {
        this(TestingPhoenixServer.getInstance());
    }

    public TestPhoenixDistributedQueries(TestingPhoenixServer server)
    {
        super(() -> createPhoenixQueryRunner(server));
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        TestingPhoenixServer.shutDown();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    public void testRenameTable()
    {
        // Phoenix does not support renaming tables
    }

    @Override
    public void testRenameColumn()
    {
        // Phoenix does not support renaming columns
    }

    @Override
    public void testDelete()
    {
        // delete not currently supported
    }

    @Override
    public void testInsert()
    {
        String query = "SELECT orderdate, orderkey, totalprice FROM orders";

        assertUpdate("CREATE TABLE test_insert WITH (ROWKEYS='orderkey') AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM test_insert", query);

        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-1)", 1); // Phoenix Upsert
        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-2)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, orderdate) VALUES (-3, DATE '2001-01-01')", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, orderdate) VALUES (-4, DATE '2001-01-02')", 1);
        assertUpdate("INSERT INTO test_insert (orderdate, orderkey) VALUES (DATE '2001-01-03', -5)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, totalprice) VALUES (-6, 1234)", 1);

        assertQuery("SELECT * FROM test_insert", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT null, -2, null"
                + " UNION ALL SELECT DATE '2001-01-01', -3, null"
                + " UNION ALL SELECT DATE '2001-01-02', -4, null"
                + " UNION ALL SELECT DATE '2001-01-03', -5, null"
                + " UNION ALL SELECT null, -6, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO test_insert (orderkey, orderdate, totalprice) " +
                        "SELECT orderkey, orderdate, totalprice FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT 2 * count(*) FROM orders");

        assertUpdate("DROP TABLE test_insert");
    }

    @Override
    public void testCommentTable()
    {
        // Phoenix connector currently does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }
}
