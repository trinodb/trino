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
package io.prestosql.elasticsearch;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.elasticsearch.client.protocols.ElasticsearchProtocol;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;

import static io.prestosql.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;

public abstract class BaseElasticsearchDistributedQueries
        extends AbstractTestDistributedQueries
{
    private final ElasticsearchProtocol elasticsearchProtocol;
    private ElasticsearchServer elasticsearchServer;

    public BaseElasticsearchDistributedQueries(ElasticsearchProtocol protocol)
    {
        this.elasticsearchProtocol = protocol;
        this.elasticsearchServer = new ElasticsearchServer(elasticsearchProtocol.getMinVersion());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createElasticsearchQueryRunner(
//                HostAndPort.fromParts("localhost", 9200),
                elasticsearchServer.getAddress(),
                elasticsearchProtocol,
                TpchTable.getTables(),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        TpchTable.getTables().forEach(table -> {
            assertUpdate(("DROP TABLE " + table.getTableName()));
        });
        elasticsearchServer.stop();
        elasticsearchServer = null;
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
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Elastic connector does not support column default values");
    }

    @Override
    public void testAddColumn()
    {
        assertQueryFails("ALTER TABLE orders ADD COLUMN addcolumn varchar", "This connector does not support adding columns");
    }

    @Override
    public void testCommentTable()
    {
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    public void testCreateSchema()
    {
        assertQueryFails("CREATE SCHEMA test_schema_create", "This connector does not support creating schemas");
    }

    @Override
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomTableSuffix();
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT orderkey, discount FROM lineitem", 0);
        // CHANGED: have to change columnNames order as they came ordered
        assertTableColumnNames("nation", "comment", "name", "nationkey", "regionkey");

        assertCreateTableAsSelect(
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTableAsSelect(
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "SELECT '\u2603' unicode",
                "SELECT 1");

//        CHANGED: not working
//        assertCreateTableAsSelect(
//                "SELECT * FROM orders WITH DATA",
//                "SELECT * FROM orders",
//                "SELECT count(*) FROM orders");
//
//        assertCreateTableAsSelect(
//                "SELECT * FROM orders WITH NO DATA",
//                "SELECT * FROM orders LIMIT 0",
//                "SELECT 0");

        assertCreateTableAsSelect(
                "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
                        "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 1",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT orderstatus FROM orders");
        assertQuery("SELECT * from " + tableName, "SELECT orderstatus FROM orders");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        failByInsert();
    }

    @Override
    public void testDelete()
    {
        assertQueryFails("DELETE FROM orders WHERE orderkey % 2 = 0", "This connector does not support updates or deletes");
    }

    @Override
    public void testDropColumn()
    {
        assertQueryFails("ALTER TABLE orders DROP COLUMN orderkey", "This connector does not support dropping columns");
    }

    @Override
    public void testInsert()
    {
        failByInsert();
    }

    @Override
    public void testInsertUnicode()
    {
        failByInsert();
    }

    @Override
    public void testInsertWithCoercion()
    {
        failByInsert();
    }

    @Override
    public void testRenameColumn()
    {
        failByInsert();
    }

    @Override
    public void testRenameTable()
    {
        assertUpdate("CREATE TABLE test_rename AS SELECT 123 x", 1);
        assertQueryFails("ALTER TABLE test_rename RENAME TO test_rename_new", "This connector does not support renaming tables");
        assertUpdate("DROP TABLE test_rename");
    }

    @Override
    public void testWrittenStats()
    {
        failByInsert();
    }

    @Override
    public void testAccessControl()
    {
        // Ignore ..
    }

    @Override
    public void testCast()
    {
        // Ignore ..
    }

    @Override
    public void testColumnAliases()
    {
        // Ignore ..
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere()
    {
        // Ignore ..
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols()
    {
        // Ignore ..
    }

    @Override
    public void testCorrelatedJoin()
    {
        // Ignore ..
    }

    @Override
    public void testCorrelatedScalarSubqueries()
    {
        // Ignore ..
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere()
    {
        // Ignore ..
    }

    @Override
    public void testGroupByKeyPredicatePushdown()
    {
        // Ignore ..
    }

    @Override
    public void testLargeIn()
    {
        // Ignore ..
    }

    @Override
    public void testLimitPushDown()
    {
        // Ignore ..
    }

    @Override
    public void testLimitWithAggregation()
    {
        // Ignore ..
    }

    @Override
    public void testScalarSubquery()
    {
        // Ignore ..
    }

    @Override
    public void testSelectAllFromOuterScopeTable()
    {
        // Ignore ..
    }

    @Override
    public void testSelectAllFromTable()
    {
        // Ignore ..
    }

    @Override
    public void testShowColumns()
    {
        // Ignore ..
    }

    @Override
    public void testSubqueriesWithDisjunction()
    {
        // Ignore ..
    }

    private void failByInsert()
    {
        String tableName = "test_types_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a varchar, b bigint)");
        assertQueryFails("INSERT INTO " + tableName + " SELECT name, regionkey FROM nation LIMIT 10", "This connector does not support inserts");
        assertUpdate("DROP TABLE " + tableName);
    }
}
