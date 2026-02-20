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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveRedirectionToHudi.
 * <p>
 * Tests Hive-to-Hudi table redirections using S3 (MinIO) storage with
 * both Copy-on-Write (COW) and Merge-on-Read (MOR) table types.
 */
@ProductTest
@RequiresEnvironment(HiveHudiRedirectionsEnvironment.class)
@TestGroup.HiveHudiRedirections
class TestHiveRedirectionToHudi
{
    private static final String HUDI_TABLE_TYPE_COPY_ON_WRITE = "cow";
    private static final String HUDI_TABLE_TYPE_MERGE_ON_READ = "mor";

    static Stream<String> hudiTableTypes()
    {
        return Stream.of(HUDI_TABLE_TYPE_COPY_ON_WRITE, HUDI_TABLE_TYPE_MERGE_ON_READ);
    }

    @ParameterizedTest
    @MethodSource("hudiTableTypes")
    void testRedirect(String hudiTableType, HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "redirect_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiTable(schemaTableName, hudiTableType, false, env);

        assertResultsEqual(
                env.executeTrino("TABLE " + hudiTableName),
                env.executeTrino("TABLE " + hiveTableName));

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
        if (HUDI_TABLE_TYPE_MERGE_ON_READ.equals(hudiTableType)) {
            env.executeSparkUpdate("DROP TABLE " + schemaTableName + "_ro");
            env.executeSparkUpdate("DROP TABLE " + schemaTableName + "_rt");
        }
    }

    @Test
    void testRedirectWithNonDefaultSchema(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "redirect_non_default_schema_" + randomNameSuffix();
        String nonDefaultSchemaName = "nondefaultschema";
        String schemaTableName = schemaTableName(nonDefaultSchemaName, tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        env.executeSparkUpdate("CREATE DATABASE IF NOT EXISTS " + nonDefaultSchemaName);
        createHudiCowTable(schemaTableName, false, env);

        assertResultsEqual(
                env.executeTrino("TABLE " + hudiTableName),
                env.executeTrino("TABLE " + hiveTableName));

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testRedirectToNonexistentCatalog(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "redirect_to_nonexistent_hudi_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        // sanity check
        assertResultsEqual(
                env.executeTrino("TABLE " + hudiTableName),
                env.executeTrino("TABLE " + hiveTableName));

        env.executeTrinoUpdate("SET SESSION hive.hudi_catalog_name = 'someweirdcatalog'");

        assertThatThrownBy(() -> env.executeTrino("TABLE " + hiveTableName))
                .hasMessageMatching(".*Table 'hive.default.redirect_to_nonexistent_hudi_.*' redirected to 'someweirdcatalog.default.redirect_to_nonexistent_hudi_.*', but the target catalog 'someweirdcatalog' does not exist");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testRedirectWithDefaultSchemaInSession(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "redirect_with_use_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        env.executeTrinoUpdate("USE hudi.default");
        assertResultsEqual(
                env.executeTrino("TABLE " + tableName), // unqualified
                env.executeTrino("TABLE " + hiveTableName));

        env.executeTrinoUpdate("USE hive.default");
        assertResultsEqual(
                env.executeTrino("TABLE " + hudiTableName),
                env.executeTrino("TABLE " + tableName)); // unqualified

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testRedirectPartitionsToUnpartitioned(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_unpartitioned_table_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThat(env.executeTrino("" +
                "SELECT _hoodie_record_key, _hoodie_partition_path, id, name, ts " +
                "FROM " + hiveTableName + " ORDER BY id"))
                .containsOnly(
                        row("1", "", 1L, "a1", 1000L),
                        row("2", "", 2L, "a2", 2000L));

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @ParameterizedTest
    @MethodSource("hudiTableTypes")
    void testRedirectPartitionsToPartitioned(String hudiTableType, HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_partitioned_table_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiTable(schemaTableName, hudiTableType, true, env);

        assertThat(env.executeTrino("" +
                "SELECT _hoodie_record_key, _hoodie_partition_path, id, name, ts, dt, hh " +
                "FROM " + hiveTableName + (HUDI_TABLE_TYPE_MERGE_ON_READ.equals(hudiTableType) ? "_rt" : "") +
                " ORDER BY id"))
                .containsOnly(
                        row("id:1", "dt=2021-12-09/hh=10", 1L, "a1", 1000L, "2021-12-09", "10"),
                        row("id:2", "dt=2021-12-09/hh=11", 2L, "a2", 1000L, "2021-12-09", "11"));

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
        if (HUDI_TABLE_TYPE_MERGE_ON_READ.equals(hudiTableType)) {
            env.executeSparkUpdate("DROP TABLE " + schemaTableName + "_ro");
            env.executeSparkUpdate("DROP TABLE " + schemaTableName + "_rt");
        }
    }

    @Test
    void testInsert(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_insert_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO " + hiveTableName + " VALUES (3, 'a3', 60, 3000)"))
                .hasMessageMatching(".*Insert query has mismatched column types: Table: \\[varchar, varchar, varchar, varchar, varchar, bigint, varchar, integer, bigint\\], Query: \\[integer, varchar\\(2\\), integer, integer\\]");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testDelete(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_delete_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("DELETE FROM " + hiveTableName + " WHERE id = 1"))
                .hasMessageMatching(".*This connector does not support modifying table rows");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testUpdate(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_update_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("UPDATE " + hiveTableName + " SET price = price + 100 WHERE id = 2"))
                .hasMessageMatching(".*This connector does not support modifying table rows");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testMerge(HiveHudiRedirectionsEnvironment env)
    {
        String sourceTableName = "hudi_merge_source_" + randomNameSuffix();
        String sourceSchemaTableName = schemaTableName("default", sourceTableName);
        String hiveSourceTableName = trinoTableName("hive", sourceSchemaTableName);

        String targetTableName = "hudi_merge_target_" + randomNameSuffix();
        String targetSchemaTableName = schemaTableName("default", targetTableName);
        String hiveTargetTableName = trinoTableName("hive", targetSchemaTableName);

        createHudiCowTable(sourceSchemaTableName, false, env);
        createHudiMorTable(targetSchemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("" +
                "MERGE INTO " + hiveTargetTableName + " t USING " + hiveSourceTableName + " s ON t.id = s.id " +
                "WHEN NOT MATCHED " +
                "    THEN INSERT (id, name, price, ts) " +
                "            VALUES (s.id, s.name, s.price, s.ts)"))
                .hasMessageMatching(".*This connector does not support modifying table rows");

        env.executeSparkUpdate("DROP TABLE " + sourceSchemaTableName);
        env.executeSparkUpdate("DROP TABLE " + targetSchemaTableName);
        env.executeSparkUpdate("DROP TABLE " + targetSchemaTableName + "_ro");
        env.executeSparkUpdate("DROP TABLE " + targetSchemaTableName + "_rt");
    }

    @Test
    void testDropTable(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_drop_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("DROP TABLE " + hiveTableName))
                .hasMessageMatching(".*This connector does not support dropping tables");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testDescribe(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_describe_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, true, env);

        assertResultsEqual(
                env.executeTrino("DESCRIBE " + hudiTableName),
                env.executeTrino("DESCRIBE " + hiveTableName));

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testShowGrants(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_show_grants_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrino(format("SHOW GRANTS ON %s", hiveTableName)))
                .hasMessageMatching(".*Table " + hiveTableName + " is redirected to " + hudiTableName + " and SHOW GRANTS is not supported with table redirections");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testGrant(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_grant_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT SELECT ON " + hiveTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching(".*Table " + hiveTableName + " is redirected to " + hudiTableName + " and GRANT is not supported with table redirections");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testRevoke(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_revoke_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE SELECT ON " + hiveTableName + " FROM ROLE PUBLIC"))
                .hasMessageMatching(".*Table " + hiveTableName + " is redirected to " + hudiTableName + " and REVOKE is not supported with table redirections");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testSetTableAuthorization(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_set_table_authorization_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + hiveTableName + " SET AUTHORIZATION ROLE PUBLIC"))
                .hasMessageMatching(".*Table " + hiveTableName + " is redirected to " + hudiTableName + " and SET TABLE AUTHORIZATION is not supported with table redirections");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    @Test
    void testDeny(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "hudi_deny_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        String hudiTableName = trinoTableName("hudi", schemaTableName);
        String hiveTableName = trinoTableName("hive", schemaTableName);

        createHudiCowTable(schemaTableName, false, env);

        assertThatThrownBy(() -> env.executeTrinoUpdate("DENY DELETE ON " + hiveTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching(".*Table " + hiveTableName + " is redirected to " + hudiTableName + " and DENY is not supported with table redirections");

        env.executeSparkUpdate("DROP TABLE " + schemaTableName);
    }

    // Helper methods

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
        assertThat(first).hasSameRowsAs(second);
        // Symmetry check
        assertThat(second).hasSameRowsAs(first);
    }

    private void createHudiCowTable(String tableName, boolean partitioned, HiveHudiRedirectionsEnvironment env)
    {
        createHudiTable(tableName, HUDI_TABLE_TYPE_COPY_ON_WRITE, partitioned, env);
    }

    private void createHudiMorTable(String tableName, boolean partitioned, HiveHudiRedirectionsEnvironment env)
    {
        createHudiTable(tableName, HUDI_TABLE_TYPE_MERGE_ON_READ, partitioned, env);
    }

    private void createHudiTable(String tableName, String tableType, boolean partitioned, HiveHudiRedirectionsEnvironment env)
    {
        String bucketName = env.getBucketName();
        if (partitioned) {
            createHudiPartitionedTable(tableName, bucketName, tableType, env);
            return;
        }
        createHudiNonPartitionedTable(tableName, bucketName, tableType, env);
    }

    private static void createHudiNonPartitionedTable(String tableName, String bucketName, String tableType, HiveHudiRedirectionsEnvironment env)
    {
        env.executeSparkUpdate(format(
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
                LOCATION 's3a://%s/%s'
                """,
                tableName,
                tableType,
                bucketName,
                tableName));

        env.executeSparkUpdate("INSERT INTO " + tableName + " VALUES (1, 'a1', 20, 1000), (2, 'a2', 40, 2000)");
    }

    private static void createHudiPartitionedTable(String tableName, String bucketName, String tableType, HiveHudiRedirectionsEnvironment env)
    {
        env.executeSparkUpdate(format(
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
                LOCATION 's3a://%s/%s'
                """,
                tableName,
                tableType,
                bucketName,
                tableName));

        env.executeSparkUpdate("INSERT INTO " + tableName + " PARTITION (dt, hh) SELECT 1 AS id, 'a1' AS name, 1000 AS ts, '2021-12-09' AS dt, '10' AS hh");
        env.executeSparkUpdate("INSERT INTO " + tableName + " PARTITION (dt = '2021-12-09', hh='11') SELECT 2, 'a2', 1000");
    }

    private static String randomNameSuffix()
    {
        return Long.toString(System.nanoTime(), 36);
    }
}
