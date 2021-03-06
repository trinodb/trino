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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;

import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A connector smoke test exercising various connector functionalities without going in depth on any of them.
 * A connector should implement {@link BaseConnectorTest} and use this class to exercise some configuration variants.
 */
public abstract class BaseConnectorSmokeTest
        extends AbstractTestQueryFramework
{
    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(NATION, REGION);

    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return connectorBehavior.hasBehaviorByDefault(this::hasBehavior);
    }

    /**
     * Ensure the tests are run with {@link DistributedQueryRunner}. E.g. {@link LocalQueryRunner} takes some
     * shortcuts, not exercising certain aspects.
     */
    @Test
    public void ensureDistributedQueryRunner()
    {
        assertThat(getQueryRunner().getNodeCount()).as("query runner node count")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    public void testSelect()
    {
        assertQuery("SELECT name FROM region");
    }

    @Test
    public void testPredicate()
    {
        assertQuery("SELECT name, regionkey FROM nation WHERE nationkey = 10");
        assertQuery("SELECT name, regionkey FROM nation WHERE nationkey BETWEEN 5 AND 15");
        assertQuery("SELECT name, regionkey FROM nation WHERE name = 'EGYPT'");
    }

    @Test
    public void testLimit()
    {
        assertQuery("SELECT name FROM region LIMIT 5");
    }

    @Test
    public void testTopN()
    {
        assertQuery("SELECT regionkey FROM nation ORDER BY name LIMIT 3");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("SELECT sum(regionkey) FROM nation");
        assertQuery("SELECT sum(nationkey) FROM nation GROUP BY regionkey");
    }

    @Test
    public void testHaving()
    {
        assertQuery("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 58", "VALUES (4, 58)");
    }

    @Test
    public void testJoin()
    {
        assertQuery("SELECT n.name, r.name FROM nation n JOIN region r on n.regionkey = r.regionkey");
    }

    @Test
    public void testCreateTable()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            assertQueryFails("CREATE TABLE xxxx (a bigint, b double)", "This connector does not support create");
            return;
        }

        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double)");
        assertThat(query("SELECT a, b FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelect()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails("CREATE TABLE xxxx (a bigint, b double) AS VALUES (42, -38.5)", "This connector does not support create");
            return;
        }

        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsert()
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            assertQueryFails("INSERT INTO region (regionkey) VALUES (42)", "This connector does not support insert");
            return;
        }

        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double)");
        assertUpdate("INSERT INTO " + tableName + " (a, b) VALUES (42, -38.5)", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDelete()
    {
        if (!hasBehavior(SUPPORTS_ROW_LEVEL_DELETE)) {
            assertQueryFails("DELETE FROM region", "This connector does not support deletes");
            return;
        }

        String tableName = "test_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM region", 5);

        // delete half the table, then delete the rest
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 2", 1);
        assertThat(query("SELECT regionkey FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES 1, 3, 4, 5");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateSchema()
    {
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            assertQueryFails("CREATE SCHEMA xxxxxx", "This connector does not support creating schemas");
            return;
        }

        String schemaName = "test_schema_create_" + randomTableSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertThat(query("SHOW SCHEMAS"))
                .skippingTypesCheck()
                .containsAll(format("VALUES '%s', '%s'", getSession().getSchema().orElseThrow(), schemaName));
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testSelectInformationSchemaTables()
    {
        assertThat(query(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .containsAll("VALUES 'nation', 'region'");
    }

    @Test
    public void testSelectInformationSchemaColumns()
    {
        assertThat(query(format("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = 'region'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
    }

    @Test
    public void testShowCreateTable()
    {
        // SHOW CREATE TABLE exercises table properties and comments, which may be skipped during regular SELECT execution
        assertThat((String) computeActual("SHOW CREATE TABLE region").getOnlyValue())
                .matches(format(
                        "CREATE TABLE %s.%s.region \\(\n" +
                                "   regionkey (bigint|decimal\\(19, 0\\)),\n" +
                                "   name varchar(\\(\\d+\\))?,\n" +
                                "   comment varchar(\\(\\d+\\))?\n" +
                                "\\)",
                        Pattern.quote(getSession().getCatalog().orElseThrow()),
                        Pattern.quote(getSession().getSchema().orElseThrow())));
    }
}
