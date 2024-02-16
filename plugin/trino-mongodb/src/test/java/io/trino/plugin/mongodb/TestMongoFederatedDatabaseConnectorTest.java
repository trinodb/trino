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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.plugin.mongodb.MongoAtlasQueryRunner.createMongoAtlasFederatedMongoQueryRunner;
import static io.trino.plugin.mongodb.TestingMongoAtlasInfoProvider.getMongoAtlasFederatedDatabaseInfo;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestMongoFederatedDatabaseConnectorTest
        extends BaseMongoConnectorSmokeTest
{
    private static final int RESOURCE_ALREADY_EXISTS = 72;
    private String clusterName;
    private String storeName;
    private String projectId;
    private TestingMongoAtlasInfoProvider.MongoAtlasFederatedDatabaseInfo clusterInfo;

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return false;
            case SUPPORTS_DELETE:
            case SUPPORTS_INSERT:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clusterName = requireSystemProperty("test.mongodb.federated-datasource.cluster-name");
        storeName = clusterName + "-store";
        projectId = requireSystemProperty("test.mongodb.federated-datasource.project-id");
        clusterInfo = getMongoAtlasFederatedDatabaseInfo();
        setup();
        return createMongoAtlasFederatedMongoQueryRunner(clusterInfo, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    /**
     * Creates required(TPCH) stores and corresponding database/collection in the Federated database
     */
    private void setup()
    {
        try (MongoClient client = MongoClients.create(clusterInfo.federatedDatabaseConnectionString())) {
            try {
                // Create underlying datasource through Atlas cluster
                // https://www.mongodb.com/docs/atlas/data-federation/admin/cli/stores/create-store/
                client.getDatabase("admin").runCommand(
                        new Document(ImmutableMap.of("createStore", storeName, "provider", "atlas", "clusterName", clusterName, "projectId", projectId)));
            }
            catch (MongoCommandException e) {
                if (e.getErrorCode() != RESOURCE_ALREADY_EXISTS) {
                    throw new RuntimeException(e);
                }
            }

            // Create TPCH database and/or missing collections
            // https://www.mongodb.com/docs/atlas/data-federation/admin/cli/collections/create-collections-views/
            String tpch = "tpch";
            Set<String> tables = ImmutableSet.copyOf(client.getDatabase(tpch).listCollectionNames());
            TpchTable.getTables().stream()
                    .filter(table -> !tables.contains(table.getTableName()))
                    .forEach(table -> {
                        try {
                            client.getDatabase(tpch).runCommand(
                                    new Document(ImmutableMap.of(
                                            "create", table.getTableName(),
                                            "dataSources", ImmutableList.of(ImmutableMap.of("storeName", storeName, "database", tpch, "collection", table.getTableName())))));
                        }
                        catch (MongoCommandException e) {
                            if (e.getErrorCode() != RESOURCE_ALREADY_EXISTS) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }

    private String requireSystemProperty(String key)
    {
        return requireNonNull(System.getProperty(key), () -> "system property not set: " + key);
    }

    @Test
    public void testAddColumn()
    {
        assertQueryFails("ALTER TABLE nation ADD COLUMN test_add_column bigint", "Adding columns is not supported on Atlas data federation");
    }

    @Test
    public void testCommentColumn()
    {
        assertQueryFails("COMMENT ON COLUMN nation.nationkey IS 'new comment'", "Setting column comments is not supported on Atlas data federation");
    }

    @Test
    public void testCommentTable()
    {
        assertQueryFails("COMMENT ON TABLE nation IS 'new comment'", "Setting table comments is not supported on Atlas data federation");
    }

    // Overridden as the failure message is different
    @Test
    @Override
    public void testCreateSchema()
    {
        assertQueryFails(createSchemaSql("test_schema"), "Creating schemas is not supported on Atlas data federation");
    }

    // Overridden as the failure message is different
    @Test
    @Override
    public void testCreateTable()
    {
        assertQueryFails("CREATE TABLE test_create (a bigint, b double, c varchar(50))", "Creating tables is not supported on Atlas data federation");
    }

    // Overridden as the failure message is different
    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        assertQueryFails("CREATE TABLE IF NOT EXISTS test_ctas AS SELECT name, regionkey FROM nation", "Creating tables with data is not supported on Atlas data federation");
    }

    @Test
    @Override // Overridden as the failure message is different
    public void testRenameTable()
    {
        assertQueryFails("ALTER TABLE nation RENAME TO yyyy", "Renaming tables is not supported on Atlas data federation");
    }

    @Test
    public void testDropColumn()
    {
        assertQueryFails("ALTER TABLE nation DROP COLUMN nationkey", "Dropping columns is not supported on Atlas data federation");
    }

    @Test
    public void testRenameColumn()
    {
        assertQueryFails("ALTER TABLE nation RENAME COLUMN nationkey TO test_rename_column", "Renaming columns is not supported on Atlas data federation");
    }

    // Overridden as the failure message is different
    @Test
    @Override
    public void testInsert()
    {
        assertQueryFails("INSERT INTO nation(nationkey) VALUES (42)", "Insert is not supported on Atlas data federation");
    }

    @Test
    public void testSetColumnType()
    {
        assertQueryFails("ALTER TABLE nation ALTER COLUMN nationkey SET DATA TYPE bigint", "Setting column data types is not supported on Atlas data federation");
    }

    // Overridden as the data types are different for few columns
    @Test
    @Override
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                .isEqualTo(format("""
                                CREATE TABLE %s.%s.orders (
                                   orderkey bigint,
                                   custkey bigint,
                                   orderstatus varchar,
                                   totalprice double,
                                   orderdate timestamp(3),
                                   orderpriority varchar,
                                   clerk varchar,
                                   shippriority bigint,
                                   comment varchar
                                )""",
                        catalog, schema));
    }

    @Test
    public void testFailAlterTable()
    {
        assertQueryFails(
                "ALTER TABLE mongodb.tpch.region RENAME TO mongodb.tpch.sampleregion",
                "Renaming tables is not supported on Atlas data federation");
    }

    @Test
    public void testFailDropTable()
    {
        assertQueryFails(
                "DROP TABLE mongodb.tpch.orders",
                "Dropping tables is not supported on Atlas data federation");
    }

    @Test
    public void testFailDeleteFromTable()
    {
        assertQueryFails(
                "DELETE FROM orders",
                "Delete is not supported on Atlas data federation");
    }

    @Test
    public void testPredicatePushdown()
    {
        for (Object[] predicatePushdown : predicatePushdownProvider()) {
            testPredicatePushdown((String) predicatePushdown[0], (String) predicatePushdown[1]);
        }
    }

    private void testPredicatePushdown(String tableName, String columnValue)
    {
        String predicatePushdownDatabase = "test";
        createPredicatePushdownTable(predicatePushdownDatabase, tableName, columnValue);
        Assertions.assertThat(query("SELECT * FROM " + predicatePushdownDatabase + "." + tableName + " WHERE col = " + columnValue))
                .isFullyPushedDown();
    }

    private void createPredicatePushdownTable(String database, String tableName, String columnValue)
    {
        getQueryRunner().execute("CREATE SCHEMA IF NOT EXISTS mongo_federated_source." + database);
        try {
            getQueryRunner().execute(format("CREATE TABLE mongo_federated_source.%s.%s AS SELECT %s col", database, tableName, columnValue));
        }
        catch (QueryFailedException e) {
            if (!e.getMessage().matches(".*Destination table .* already exists")) {
                throw new RuntimeException(e);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (MongoClient client = MongoClients.create(clusterInfo.federatedDatabaseConnectionString())) {
            try {
                client.getDatabase(database).runCommand(
                        new Document(ImmutableMap.of(
                                "create", tableName,
                                "dataSources", ImmutableList.of(ImmutableMap.of("storeName", storeName, "database", database, "collection", tableName)))));
            }
            catch (MongoCommandException e) {
                if (e.getErrorCode() != RESOURCE_ALREADY_EXISTS) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static Object[][] predicatePushdownProvider()
    {
        // Any changes in the returned object format should be reflected in TestMongoAtlasInstanceCleaner.RETAINED_COLLECTIONS
        return new Object[][] {
                {"test_predicate_pushdown_boolean", "true"},
                {"test_predicate_pushdown_tinyint", "tinyint '1'"},
                {"test_predicate_pushdown_smallint", "smallint '2'"},
                {"test_predicate_pushdown_integer", "integer '3'"},
                {"test_predicate_pushdown_bigint", "bigint '4'"},
                {"test_predicate_pushdown_string", "'test'"},
                {"test_predicate_pushdown_objectid", "objectid('6216f0c6c432d45190f25e7c')"},
                {"test_predicate_pushdown_date", "date '1970-01-01'"},
                {"test_predicate_pushdown_timestamp1", "timestamp '1970-01-01 00:00:00.000'"},
                {"test_predicate_pushdown_timestamp2", "timestamp '1970-01-01 00:00:00.000 UTC'"}
        };
    }

    @Test
    @Override
    public void testProjectionPushdown()
    {
        abort("Creating tables is not supported on Atlas data federation");
    }

    @Test
    @Override
    public void testReadDottedField()
    {
        abort("Creating tables is not supported on Atlas data federation");
    }

    @Test
    @Override
    public void testReadDollarPrefixedField()
    {
        abort("Creating tables is not supported on Atlas data federation");
    }

    @Test
    @Override
    public void testProjectionPushdownWithHighlyNestedData()
    {
        abort("Creating tables is not supported on Atlas data federation");
    }

    @Test
    public void testLimitPushdown()
    {
        Assertions.assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // Make sure LIMIT 0 returns empty result because cursor.limit(0) means no limit in MongoDB
        Assertions.assertThat(query("SELECT name FROM nation LIMIT 0")).returnsEmptyResult();

        // MongoDB doesn't support limit number greater than integer max
        Assertions.assertThat(query("SELECT name FROM nation LIMIT 2147483647")).isFullyPushedDown();
        Assertions.assertThat(query("SELECT name FROM nation LIMIT 2147483648")).isNotFullyPushedDown(LimitNode.class);
    }

    @Test
    public void testSystemSchemas()
    {
        // Ensures that system schemas are inaccessible
        assertQueryReturnsEmptyResult("SHOW SCHEMAS LIKE 'admin'");
        assertQueryReturnsEmptyResult("SHOW SCHEMAS LIKE 'config'");
        assertQueryReturnsEmptyResult("SHOW SCHEMAS LIKE 'local'");
    }
}
