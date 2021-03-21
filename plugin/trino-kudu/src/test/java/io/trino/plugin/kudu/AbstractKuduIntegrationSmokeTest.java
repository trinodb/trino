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
package io.trino.plugin.kudu;

import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.AbstractTestIntegrationSmokeTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public abstract class AbstractKuduIntegrationSmokeTest
        // TODO extend BaseConnectorTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingKuduServer kuduServer;

    protected abstract Optional<String> getKuduSchemaEmulationPrefix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunnerTpch(kuduServer, getKuduSchemaEmulationPrefix(), CUSTOMER, NATION, ORDERS, REGION);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        kuduServer.close();
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        String extra = "nullable, encoding=auto, compression=default";
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", extra, "")
                .row("custkey", "bigint", extra, "")
                .row("orderstatus", "varchar", extra, "")
                .row("totalprice", "double", extra, "")
                .row("orderdate", "varchar", extra, "")
                .row("orderpriority", "varchar", extra, "")
                .row("clerk", "varchar", extra, "")
                .row("shippriority", "integer", extra, "")
                .row("comment", "varchar", extra, "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE kudu\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint WITH ( nullable = true ),\n" +
                        "   custkey bigint WITH ( nullable = true ),\n" +
                        "   orderstatus varchar WITH ( nullable = true ),\n" +
                        "   totalprice double WITH ( nullable = true ),\n" +
                        "   orderdate varchar WITH ( nullable = true ),\n" +
                        "   orderpriority varchar WITH ( nullable = true ),\n" +
                        "   clerk varchar WITH ( nullable = true ),\n" +
                        "   shippriority integer WITH ( nullable = true ),\n" +
                        "   comment varchar WITH ( nullable = true )\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   number_of_replicas = 3,\n" +
                        "   partition_by_hash_buckets = 2,\n" +
                        "   partition_by_hash_columns = ARRAY['row_uuid'],\n" +
                        "   partition_by_range_columns = ARRAY['row_uuid'],\n" +
                        "   range_partitions = '[{\"lower\":null,\"upper\":null}]'\n" +
                        ")");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_show_create_table (\n" +
                "id INT WITH (primary_key=true),\n" +
                "user_name VARCHAR\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2," +
                " number_of_replicas = 1\n" +
                ")");

        MaterializedResult result = computeActual("SHOW CREATE TABLE test_show_create_table");
        String sqlStatement = (String) result.getOnlyValue();
        String tableProperties = sqlStatement.split("\\)\\s*WITH\\s*\\(")[1];
        assertTableProperty(tableProperties, "number_of_replicas", "1");
        assertTableProperty(tableProperties, "partition_by_hash_columns", Pattern.quote("ARRAY['id']"));
        assertTableProperty(tableProperties, "partition_by_hash_buckets", "2");

        assertUpdate("DROP TABLE test_show_create_table");
    }

    @Test
    public void testRowDelete()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_row_delete (" +
                "id INT WITH (primary_key=true), " +
                "second_id INT, " +
                "user_name VARCHAR" +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_row_delete VALUES (0, 1, 'user0'), (3, 4, 'user2'), (2, 3, 'user2'), (1, 2, 'user1')", 4);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 4");

        assertUpdate("DELETE FROM test_row_delete WHERE second_id = 4", 1);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 3");

        assertUpdate("DELETE FROM test_row_delete WHERE user_name = 'user1'", 1);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 2");

        assertUpdate("DELETE FROM test_row_delete WHERE id = 0", 1);
        assertQuery("SELECT * FROM test_row_delete", "VALUES (2, 3, 'user2')");

        assertUpdate("DROP TABLE test_row_delete");
    }

    @Test
    public void testProjection()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_projection (" +
                "id INT WITH (primary_key=true), " +
                "user_name VARCHAR " +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_projection VALUES (0, 'user0'), (2, 'user2'), (1, 'user1')", 3);

        assertQuery("SELECT id, 'test' FROM test_projection ORDER BY id", "VALUES (0, 'test'), (1, 'test'), (2, 'test')");

        assertUpdate("DROP TABLE test_projection");
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isNotFullyPushedDown(LimitNode.class); // Use high limit for result determinism
    }

    private void assertTableProperty(String tableProperties, String key, String regexValue)
    {
        assertTrue(Pattern.compile(key + "\\s*=\\s*" + regexValue + ",?\\s+").matcher(tableProperties).find(),
                "Not found: " + key + " = " + regexValue + " in " + tableProperties);
    }

    @Test
    public void testMergeSimpleSelect()
    {
        String targetTable = "simple_select_target";
        String sourceTable = "simple_select_source";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR)" +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR)" +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", sourceTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                    4);

            assertQuery("SELECT * FROM " + targetTable, "SELECT * FROM (VALUES('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        }
    }

    @Test
    public void testMergeMultipleOperations()
    {
        String targetTable = "merge_multiple";
        try {
            int targetCustomerCount = 10;
            String originalInsertFirstHalf = IntStream.range(0, targetCustomerCount / 2)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                    .collect(Collectors.joining(", "));
            String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                    .collect(Collectors.joining(", "));

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount);

            String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                    .collect(Collectors.joining(", "));

            assertUpdate(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, firstMergeSource) +
                    "    ON t.customer = s.customer" +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address",
                    targetCustomerCount / 2);

            assertQuery(
                    "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                    format("SELECT * FROM (VALUES %s, %s) AS v(customer, purchases, zipcode, spouse, address)", originalInsertFirstHalf, firstMergeSource));

            String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                    .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                    .collect(Collectors.joining(", "));
            assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

            String secondMergeSource = IntStream.range(0, targetCustomerCount * 3 / 2)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                    .collect(Collectors.joining(", "));

            assertUpdate(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, secondMergeSource) +
                    "    ON t.customer = s.customer" +
                    "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                    "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                    "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, zipcode, spouse, address) VALUES(s.customer, s.purchases, s.zipcode, s.spouse, s.address)",
                    targetCustomerCount * 3 / 2);

            String updatedBeginning = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 60000, intValue, intValue))
                    .collect(Collectors.joining(", "));
            String updatedMiddle = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                    .collect(Collectors.joining(", "));
            String updatedEnd = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                    .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                    .collect(Collectors.joining(", "));

            assertQuery(
                    "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                    format("SELECT * FROM (VALUES %s, %s, %s) AS v(customer, purchases, zipcode, spouse, address)", updatedBeginning, updatedMiddle, updatedEnd));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
        }
    }

    @Test
    public void testMergeInsertAll()
    {
        String targetTable = "merge_update_all";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR)" +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 3" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable), 2);

            assertUpdate(format("MERGE INTO %s t USING ", targetTable) +
                    "(SELECT * FROM (VALUES ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire'))) AS s(customer, purchases, address)" +
                    "ON (t.customer = s.customer)" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                    2);

            assertQuery(
                    "SELECT * FROM " + targetTable,
                    "SELECT * FROM (VALUES('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
        }
    }

    @Test
    public void testMergeAllColumnsUpdated()
    {
        String targetTable = "merge_all_columns_updated_target";
        String sourceTable = "merge_all_columns_updated_source";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", targetTable));
            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Devon'), ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge')", targetTable), 4);

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", sourceTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire'), ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Ed', 7, 'Etherville')", sourceTable), 4);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                            "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_updated'), purchases = s.purchases + t.purchases, address = s.address",
                    4);

            assertQuery(
                    "SELECT * FROM " + targetTable,
                    "SELECT * FROM (VALUES ('Dave_updated', 22, 'Darbyshire'), ('Aaron_updated', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol_updated', 12, 'Centreville'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        }
    }

    @Test
    public void testMergeAllMatchesDeleted()
    {
        String targetTable = "merge_all_matches_deleted_target";
        String sourceTable = "merge_all_matches_deleted_source";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", sourceTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')", sourceTable), 4);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                            "    WHEN MATCHED THEN DELETE",
                    4);

            assertQuery("SELECT * FROM " + targetTable, "SELECT * FROM (VALUES ('Bill', 7, 'Buena'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        }
    }

    @Test
    public void testMergeLimes()
    {
        String targetTable = "merge_with_various_formats";
        String sourceTable = "merge_simple_source";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchase VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 5" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable), 3);
            assertQuery("SELECT * FROM " + targetTable, "SELECT * FROM (VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles'))");

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchase VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 5" +
                    ")", sourceTable));

            assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable), 3);

            String sql = format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                    "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)";

            assertUpdate(sql, 3);

            assertQuery("SELECT * FROM " + targetTable, "SELECT * FROM (VALUES ('Dave', 'dates'), row('Carol_Craig', 'candles'), row('Joe', 'jellybeans'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        }
    }
}
