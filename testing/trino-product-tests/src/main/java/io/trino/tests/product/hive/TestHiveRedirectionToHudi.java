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
package io.trino.tests.product.hive;

import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE_HUDI_REDIRECTIONS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHudi;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveRedirectionToHudi
        extends ProductTest
{
    private String bucketName;
    private static final String HUDI_TABLE_TYPE_COPY_ON_WRITE = "cow";
    private static final String HUDI_TABLE_TYPE_MERGE_ON_READ = "mor";

    @BeforeMethodWithContext
    public void setUp()
    {
        bucketName = System.getenv().getOrDefault("S3_BUCKET", "test-bucket");
    }

    @DataProvider
    public Object[][] testHudiTableTypesDataDataProvider()
    {
        return new Object[][] {
                {HUDI_TABLE_TYPE_COPY_ON_WRITE},
                {HUDI_TABLE_TYPE_MERGE_ON_READ}
        };
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS}, dataProvider = "testHudiTableTypesDataDataProvider")
    public void testRedirect(String hudiTableType)
    {
        String tableName = "redirect_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiTable(schemaTableName, hudiTableType, false);

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hudiTableName),
                onTrino().executeQuery("TABLE " + hiveTableName));

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
        if (HUDI_TABLE_TYPE_MERGE_ON_READ.equals(hudiTableType)) {
            onHudi().executeQuery("DROP TABLE " + schemaTableName + "_ro");
            onHudi().executeQuery("DROP TABLE " + schemaTableName + "_rt");
        }
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectWithNonDefaultSchema()
    {
        String tableName = "redirect_non_default_schema_" + randomNameSuffix();
        String nonDefaultSchemaName = "nondefaultschema";
        String schemaTableName = schemaTableName(nonDefaultSchemaName, tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        onHudi().executeQuery("CREATE DATABASE IF NOT EXISTS " + nonDefaultSchemaName);
        createHudiCowTable(schemaTableName, false);

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hudiTableName),
                onTrino().executeQuery("TABLE " + hiveTableName));

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectToNonexistentCatalog()
    {
        String tableName = "redirect_to_nonexistent_hudi_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        // sanity check
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hudiTableName),
                onTrino().executeQuery("TABLE " + hiveTableName));

        onTrino().executeQuery("SET SESSION hive.hudi_catalog_name = 'someweirdcatalog'");

        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + hiveTableName))
                .hasMessageMatching(".*Table 'hive.default.redirect_to_nonexistent_hudi_.*' redirected to 'someweirdcatalog.default.redirect_to_nonexistent_hudi_.*', but the target catalog 'someweirdcatalog' does not exist");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectWithDefaultSchemaInSession()
    {
        String tableName = "redirect_with_use_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        onTrino().executeQuery("USE hudi.default");
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + tableName), // unqualified
                onTrino().executeQuery("TABLE " + hiveTableName));

        onTrino().executeQuery("USE hive.default");
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hudiTableName),
                onTrino().executeQuery("TABLE " + tableName)); // unqualified

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectPartitionsToUnpartitioned()
    {
        String tableName = "hudi_unpartitioned_table_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertThat(onTrino().executeQuery("" +
                "SELECT _hoodie_record_key, _hoodie_partition_path, id, name, ts " +
                "FROM " + hiveTableName + " ORDER BY id"))
                .containsOnly(
                        row("1", "", 1, "a1", 1000),
                        row("2", "", 2, "a2", 2000));

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS}, dataProvider = "testHudiTableTypesDataDataProvider")
    public void testRedirectPartitionsToPartitioned(String hudiTableType)
    {
        String tableName = "hudi_partitioned_table_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiTable(schemaTableName, hudiTableType, true);

        assertThat(onTrino().executeQuery("" +
                "SELECT _hoodie_record_key, _hoodie_partition_path, id, name, ts, dt, hh " +
                "FROM " + hiveTableName + (HUDI_TABLE_TYPE_MERGE_ON_READ.equals(hudiTableType) ? "_rt" : "") +
                " ORDER BY id"))
                .containsOnly(
                        row("id:1", "dt=2021-12-09/hh=10", 1, "a1", 1000, "2021-12-09", "10"),
                        row("id:2", "dt=2021-12-09/hh=11", 2, "a2", 1000, "2021-12-09", "11"));

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
        if (HUDI_TABLE_TYPE_MERGE_ON_READ.equals(hudiTableType)) {
            onHudi().executeQuery("DROP TABLE " + schemaTableName + "_ro");
            onHudi().executeQuery("DROP TABLE " + schemaTableName + "_rt");
        }
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testInsert()
    {
        String tableName = "hudi_insert_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO " + hiveTableName + " VALUES (3, 'a3', 60, 3000)"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Insert query has mismatched column types: Table: [varchar, varchar, varchar, varchar, varchar, bigint, varchar, integer, bigint], Query: [integer, varchar(2), integer, integer]");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDelete()
    {
        String tableName = "hudi_delete_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM " + hiveTableName + " WHERE id = 1"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): This connector does not support modifying table rows");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testUpdate()
    {
        String tableName = "hudi_update_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("UPDATE " + hiveTableName + " SET price = price + 100 WHERE id = 2"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): This connector does not support modifying table rows");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testMerge()
    {
        String sourceTableName = "hudi_merge_source_" + randomNameSuffix();
        String sourceSchemaTableName = schemaTableName("default", sourceTableName);
        String hiveSourceTableName = trinoTableName("hive", sourceSchemaTableName);

        String targetTableName = "hudi_merge_target_" + randomNameSuffix();
        String targetSchemaTableName = schemaTableName("default", targetTableName);
        String hiveTargetTableName = trinoTableName("hive", targetSchemaTableName);

        createHudiCowTable(sourceSchemaTableName, false);
        createHudiMorTable(targetSchemaTableName, false);

        assertQueryFailure(() -> (onTrino().executeQuery("" +
                "MERGE INTO " + hiveTargetTableName + " t USING " + hiveSourceTableName + " s ON t.id = s.id " +
                "WHEN NOT MATCHED " +
                "    THEN INSERT (id, name, price, ts) " +
                "            VALUES (s.id, s.name, s.price, s.ts)")))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): This connector does not support modifying table rows");

        onHudi().executeQuery("DROP TABLE " + sourceSchemaTableName);
        onHudi().executeQuery("DROP TABLE " + targetSchemaTableName);
        onHudi().executeQuery("DROP TABLE " + targetSchemaTableName + "_ro");
        onHudi().executeQuery("DROP TABLE " + targetSchemaTableName + "_rt");
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDropTable()
    {
        String tableName = "hudi_drop_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("DROP TABLE  " + hiveTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): This connector does not support dropping tables");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDescribe()
    {
        String tableName = "hudi_describe_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, true);

        assertResultsEqual(
                onTrino().executeQuery("DESCRIBE " + hudiTableName),
                onTrino().executeQuery("DESCRIBE " + hiveTableName));

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowGrants()
    {
        String tableName = "hudi_show_grants_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery(format("SHOW GRANTS ON %s", hiveTableName)))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + hudiTableName + " and SHOW GRANTS is not supported with table redirections");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testGrant()
    {
        String tableName = "hudi_grant_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("GRANT SELECT ON " + hiveTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + hudiTableName + " and GRANT is not supported with table redirections");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRevoke()
    {
        String tableName = "hudi_revoke_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("REVOKE SELECT ON " + hiveTableName + " FROM ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + hudiTableName + " and REVOKE is not supported with table redirections");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testSetTableAuthorization()
    {
        String tableName = "hudi_set_table_authorization_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + hiveTableName + " SET AUTHORIZATION ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + hudiTableName + " and SET TABLE AUTHORIZATION is not supported with table redirections");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    @Test(groups = {HIVE_HUDI_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDeny()
    {
        String tableName = "hudi_deny_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("DENY DELETE ON " + hiveTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + hudiTableName + " and DENY is not supported with table redirections");

        onHudi().executeQuery("DROP TABLE " + schemaTableName);
    }

    private static String schemaTableName(String schema, String tableName)
    {
        return "%s.%s".formatted(schema, tableName);
    }

    private static String trinoTableName(String catalog, String schemaTableName)
    {
        return "%s.%s".formatted(catalog, schemaTableName);
    }

    private static void assertResultsEqual(QueryResult first, QueryResult second)
    {
        assertThat(first).containsOnly(second.rows().stream()
                .map(QueryAssert.Row::new)
                .collect(toImmutableList()));

        // just for symmetry
        assertThat(second).containsOnly(first.rows().stream()
                .map(QueryAssert.Row::new)
                .collect(toImmutableList()));
    }

    private void createHudiCowTable(String tableName, boolean partitioned)
    {
        createHudiTable(tableName, HUDI_TABLE_TYPE_COPY_ON_WRITE, partitioned);
    }

    private void createHudiMorTable(String tableName, boolean partitioned)
    {
        createHudiTable(tableName, HUDI_TABLE_TYPE_MERGE_ON_READ, partitioned);
    }

    private void createHudiTable(String tableName, String tableType, boolean partitioned)
    {
        if (partitioned) {
            createHudiPartitionedTable(tableName, bucketName, tableType);
            return;
        }

        createHudiNonPartitionedTable(tableName, bucketName, tableType);
    }

    private static void createHudiNonPartitionedTable(String tableName, String bucketName, String tableType)
    {
        onHudi().executeQuery(format(
                """
                        CREATE TABLE %s (
                          id bigint,
                          name string,
                          price int,
                          ts bigint)
                        USING hudi
                        TBLPROPERTIES (
                          type = '%s',
                          primaryKey = 'id',
                          preCombineField = 'ts')
                        LOCATION 's3://%s/%s'""",
                tableName,
                tableType,
                bucketName,
                tableName));

        onHudi().executeQuery("INSERT INTO " + tableName + " VALUES (1, 'a1', 20, 1000), (2, 'a2', 40, 2000)");
    }

    private static void createHudiPartitionedTable(String tableName, String bucketName, String tableType)
    {
        onHudi().executeQuery(format(
                """
                        CREATE TABLE %s (
                          id bigint,
                          name string,
                          ts bigint,
                          dt string,
                          hh string)
                        USING hudi
                        TBLPROPERTIES (
                          type = '%s',
                          primaryKey = 'id',
                          preCombineField = 'ts')
                        PARTITIONED BY (dt, hh)
                        LOCATION 's3://%s/%s'""",
                tableName,
                tableType,
                bucketName,
                tableName));

        onHudi().executeQuery("INSERT INTO " + tableName + " PARTITION (dt, hh) SELECT 1 AS id, 'a1' AS name, 1000 AS ts, '2021-12-09' AS dt, '10' AS hh");
        onHudi().executeQuery("INSERT INTO " + tableName + " PARTITION (dt = '2021-12-09', hh='11') SELECT 2, 'a2', 1000");
    }
}
