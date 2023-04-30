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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;

public abstract class BaseDeltaLakeRegisterTableProcedureTest
        extends AbstractTestQueryFramework
{
    protected static final String CATALOG_NAME = "delta_lake";
    protected static final String SCHEMA = "test_delta_lake_register_table_" + randomNameSuffix();

    private Path dataDirectory;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");
        this.metastore = createTestMetastore(dataDirectory);

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.of(new TestingDeltaLakeMetastoreModule(metastore)), Optional.empty(), EMPTY_MODULE));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("delta.unique-table-location", "true")
                .put("delta.register-table-procedure.enabled", "true")
                .buildOrThrow();

        queryRunner.createCatalog(CATALOG_NAME, CONNECTOR_NAME, connectorProperties);
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);

        return queryRunner;
    }

    protected abstract HiveMetastore createTestMetastore(Path dataDirectory);

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
    }

    @Test
    public void testRegisterTable()
    {
        String tableName = "test_register_table_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        String showCreateTableOld = (String) computeScalar("SHOW CREATE TABLE " + tableName);

        // Drop table from metastore and use the table content to register a table
        metastore.dropTable(SCHEMA, tableName, false);
        // Verify that dropTableFromMetastore actually works
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertQuerySucceeds(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, tableName, tableLocation));
        String showCreateTableNew = (String) computeScalar("SHOW CREATE TABLE " + tableName);

        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterPartitionedTable()
    {
        String tableName = "test_register_partitioned_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " WITH(partitioned_by = ARRAY['part']) AS SELECT 1 AS data, 'a' AS part", 1);

        String tableLocation = getTableLocation(tableName);

        metastore.dropTable(SCHEMA, tableName, false);

        assertUpdate(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, tableName, tableLocation));

        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName)).contains("partitioned_by = ARRAY['part']");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'a')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithComments()
    {
        String tableName = "test_register_table_with_comments_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " (a, b, c) COMMENT 'my-table-comment' AS VALUES (1, 'INDIA', true)");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        // Drop table from metastore and use the table content to register a table
        metastore.dropTable(SCHEMA, tableName, false);

        assertQuerySucceeds("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertThat(getTableComment(tableName)).isEqualTo("my-table-comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithDifferentTableName()
    {
        String tableName = "test_register_table_with_different_table_name_old_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String showCreateTableOld = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        String tableLocation = getTableLocation(tableName);
        // Drop table from metastore and use the table content to register a table
        metastore.dropTable(SCHEMA, tableName, false);

        String tableNameNew = "test_register_table_with_different_table_name_new_" + randomNameSuffix();
        assertQuerySucceeds(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableNameNew, tableLocation));
        String showCreateTableNew = (String) computeScalar("SHOW CREATE TABLE " + tableNameNew);

        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew.replaceFirst(tableNameNew, tableName));
        assertQuery("SELECT * FROM " + tableNameNew, "VALUES (1, 'INDIA', true)");

        assertUpdate(format("DROP TABLE %s", tableNameNew));
    }

    @Test
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        String tableName = "test_register_table_with_trailing_space_" + randomNameSuffix();
        String tableLocationWithTrailingSpace = dataDirectory.toUri() + "/" + tableName + " ";

        assertQuerySucceeds(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS a, 'INDIA' AS b, true AS c", tableName, tableLocationWithTrailingSpace));
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        assertThat(getTableLocation(tableName)).isEqualTo(tableLocationWithTrailingSpace);

        String registeredTableName = "test_register_table_with_trailing_space_" + randomNameSuffix();
        assertQuerySucceeds(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, registeredTableName, tableLocationWithTrailingSpace));
        assertQuery("SELECT * FROM " + registeredTableName, "VALUES (1, 'INDIA', true)");

        assertThat(getTableLocation(registeredTableName)).isEqualTo(tableLocationWithTrailingSpace);

        assertUpdate("DROP TABLE " + registeredTableName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterEmptyTable()
    {
        String tableName = "test_register_table_with_no_data_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + "(a INT, b VARCHAR, c BOOLEAN)");

        String tableLocation = getTableLocation(tableName);
        metastore.dropTable(SCHEMA, tableName, false);

        assertQuerySucceeds(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, tableName, tableLocation));

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithInvalidDeltaTable()
            throws Exception
    {
        String tableName = "test_register_table_with_no_transaction_log_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = "test_register_table_with_no_transaction_log_new_" + randomNameSuffix();

        // Delete files under transaction log directory and put an invalid log file to verify register_table call fails
        String transactionLogDir = new URI(getTransactionLogDir(tableLocation)).getPath();
        deleteDirectoryContents(Path.of(transactionLogDir), ALLOW_INSECURE);
        new File("/" + getTransactionLogJsonEntryPath(transactionLogDir, 0).path()).createNewFile();

        assertQueryFails(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, tableNameNew, tableLocation),
                ".*Metadata not found in transaction log for (.*)");

        deleteRecursively(Path.of(new URI(tableLocation).getPath()), ALLOW_INSECURE);
        metastore.dropTable(SCHEMA, tableName, false);
    }

    @Test
    public void testRegisterTableWithNoTransactionLog()
            throws Exception
    {
        String tableName = "test_register_table_with_no_transaction_log_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = "test_register_table_with_no_transaction_log_new_" + randomNameSuffix();

        // Delete files under transaction log directory to verify register_table call fails
        deleteDirectoryContents(Path.of(new URI(getTransactionLogDir(tableLocation)).getPath()), ALLOW_INSECURE);

        assertQueryFails(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, tableNameNew, tableLocation),
                ".*No transaction log found in location (.*)");

        deleteRecursively(Path.of(new URI(tableLocation).getPath()), ALLOW_INSECURE);
        metastore.dropTable(SCHEMA, tableName, false);
    }

    @Test
    public void testRegisterTableWithNonExistingTableLocation()
    {
        String tableName = "test_register_table_with_non_existing_table_location_" + randomNameSuffix();
        String tableLocation = "/test/delta-lake/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, tableName, tableLocation),
                ".*No transaction log found in location (.*).*");
    }

    @Test
    public void testRegisterTableWithNonExistingSchema()
    {
        String tableLocation = "/test/delta-lake/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA + "_new", "delta_table_1", tableLocation),
                "Schema (.*) not found");
    }

    @Test
    public void testRegisterTableWithExistingTable()
    {
        String tableName = "test_register_table_with_existing_table_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);

        assertQueryFails(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, tableName, tableLocation),
                ".*Table already exists: '(.*)'.*");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithInvalidUriScheme()
    {
        String tableName = "test_register_table_with_invalid_uri_scheme_" + randomNameSuffix();
        String tableLocation = "invalid://hadoop-master:9000/test/delta-lake/hive/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, tableName, tableLocation),
                ".*Failed checking table location (.*)");
    }

    @Test
    public void testRegisterTableWithInvalidParameter()
    {
        String tableName = "test_register_table_with_invalid_parameter_" + randomNameSuffix();
        String tableLocation = "/test/delta-lake/hive/table1/";

        assertQueryFails(format("CALL system.register_table('%s', '%s')", SCHEMA, tableName),
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails(format("CALL system.register_table('%s')", SCHEMA),
                ".*'TABLE_NAME' is missing.*");
        assertQueryFails("CALL system.register_table()",
                ".*'SCHEMA_NAME' is missing.*");

        assertQueryFails(format("CALL system.register_table(NULL, '%s', '%s')", tableName, tableLocation),
                ".*schema_name cannot be null or empty.*");
        assertQueryFails(format("CALL system.register_table('%s', NULL, '%s')", SCHEMA, tableLocation),
                ".*table_name cannot be null or empty.*");
        assertQueryFails(format("CALL system.register_table('%s', '%s', NULL)", SCHEMA, tableName),
                ".*table_location cannot be null or empty.*");

        assertQueryFails(format("CALL system.register_table('', '%s', '%s')", tableName, tableLocation),
                ".*schema_name cannot be null or empty.*");
        assertQueryFails(format("CALL system.register_table('%s', '', '%s')", SCHEMA, tableLocation),
                ".*table_name cannot be null or empty.*");
        assertQueryFails(format("CALL system.register_table('%s', '%s', '')", SCHEMA, tableName),
                ".*table_location cannot be null or empty.*");
    }

    @Test
    public void testRegisterUnregisteredTable()
    {
        // Verify register_table procedure can register the unregistered table
        String tableName = "test_unregister_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);
        String tableLocation = getTableLocation(tableName);

        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");
        assertQueryFails("SELECT * FROM " + tableName, ".* Table .* does not exist");

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    protected String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    private String getTableComment(String tableName)
    {
        return (String) computeScalar(format(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '%s' AND schema_name = '%s' AND table_name = '%s'",
                CATALOG_NAME,
                SCHEMA,
                tableName));
    }
}
