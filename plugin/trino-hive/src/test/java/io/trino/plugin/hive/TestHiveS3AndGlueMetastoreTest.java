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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.trino.plugin.hive.BaseS3AndGlueMetastoreTest.LocationPattern.DOUBLE_SLASH;
import static io.trino.plugin.hive.BaseS3AndGlueMetastoreTest.LocationPattern.TRIPLE_SLASH;
import static io.trino.plugin.hive.BaseS3AndGlueMetastoreTest.LocationPattern.TWO_TRAILING_SLASHES;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveS3AndGlueMetastoreTest
        extends BaseS3AndGlueMetastoreTest
{
    public TestHiveS3AndGlueMetastoreTest()
    {
        super("partitioned_by", "external_location", requireEnv("S3_BUCKET"));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin"))));
        QueryRunner queryRunner = HiveQueryRunner.builder(session)
                .addExtraProperty("sql.path", "hive.functions")
                .addExtraProperty("sql.default-function-catalog", "hive")
                .addExtraProperty("sql.default-function-schema", "functions")
                .setCreateTpchSchemas(false)
                .addHiveProperty("hive.metastore", "glue")
                .addHiveProperty("hive.metastore.glue.default-warehouse-dir", schemaPath())
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperty("hive.non-managed-table-writes-enabled", "true")
                .addHiveProperty("hive.partition-projection-enabled", "true")
                .addHiveProperty("fs.hadoop.enabled", "false")
                .addHiveProperty("fs.native-s3.enabled", "true")
                .build();
        queryRunner.execute("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaPath() + "')");
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS functions");

        metastore = getConnectorService(queryRunner, GlueHiveMetastore.class);

        return queryRunner;
    }

    private Session createSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRoles(role.map(selectedRole -> ImmutableMap.of("hive", selectedRole))
                                .orElse(ImmutableMap.of()))
                        .build())
                .setCatalog("hive")
                .setSchema(schemaName)
                .build();
    }

    @Override
    protected Session sessionForOptimize()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "non_transactional_optimize_enabled", "true")
                .build();
    }

    @Override
    protected void validateDataFiles(String partitionColumn, String tableName, String location)
    {
        getActiveFiles(tableName).forEach(dataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            String partitionPart = partitionColumn.isEmpty() ? "" : partitionColumn + "=[a-z0-9]+/";
            assertThat(dataFile).matches("^" + Pattern.quote(locationDirectory) + partitionPart + "[a-zA-Z0-9_-]+$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        // No metadata files for Hive
    }

    @Override
    protected Set<String> getAllDataFilesFromTableDirectory(String tableLocation)
    {
        return new HashSet<>(getTableFiles(tableLocation));
    }

    @Override
    protected void validateFilesAfterOptimize(String location, Set<String> initialFiles, Set<String> updatedFiles)
    {
        assertThat(updatedFiles).hasSizeLessThan(initialFiles.size());
        assertThat(getAllDataFilesFromTableDirectory(location)).isEqualTo(updatedFiles);
    }

    @Override // Row-level modifications are not supported for Hive tables
    protected void testBasicOperationsWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        String tableName = "test_basic_operations_" + randomNameSuffix();
        String location = locationPattern.locationForTable(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['col_int']" : "");

        String create = "CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (external_location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)";
        if (locationPattern == DOUBLE_SLASH || locationPattern == TRIPLE_SLASH || locationPattern == TWO_TRAILING_SLASHES) {
            assertQueryFails(create, "\\QUnsupported location that cannot be internally represented: " + location);
            return;
        }
        assertUpdate(create, 3);
        try (UncheckedCloseable ignored = onClose("DROP TABLE " + tableName)) {
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");

            String actualTableLocation = getTableLocation(tableName);
            assertThat(actualTableLocation).isEqualTo(location);

            assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

            assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
            validateDataFiles(partitioned ? "col_int" : "", tableName, actualTableLocation);
        }
    }

    @Test
    public void testBasicOperationsWithProvidedTableLocationNonCTAS()
    {
        for (LocationPattern locationPattern : LocationPattern.values()) {
            testBasicOperationsWithProvidedTableLocationNonCTAS(false, locationPattern);
            testBasicOperationsWithProvidedTableLocationNonCTAS(true, locationPattern);
        }
    }

    private void testBasicOperationsWithProvidedTableLocationNonCTAS(boolean partitioned, LocationPattern locationPattern)
    {
        // this test needed, because execution path for CTAS and simple create is different
        String tableName = "test_basic_operations_" + randomNameSuffix();
        String location = locationPattern.locationForTable(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['col_int']" : "");

        String create = "CREATE TABLE " + tableName + "(col_str varchar, col_int integer) WITH (external_location = '" + location + "' " + partitionQueryPart + ")";
        if (locationPattern == DOUBLE_SLASH || locationPattern == TRIPLE_SLASH || locationPattern == TWO_TRAILING_SLASHES) {
            assertQueryFails(create, "\\QUnsupported location that cannot be internally represented: " + location);
            return;
        }
        assertUpdate(create);
        try (UncheckedCloseable ignored = onClose("DROP TABLE " + tableName)) {
            String actualTableLocation = getTableLocation(tableName);
            assertThat(actualTableLocation).isEqualTo(location);

            assertUpdate("INSERT INTO " + tableName + " VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)", 4);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

            assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
            validateDataFiles(partitioned ? "col_int" : "", tableName, actualTableLocation);
        }
    }

    @Override // Row-level modifications are not supported for Hive tables
    protected void testBasicOperationsWithProvidedSchemaLocation(boolean partitioned, LocationPattern locationPattern)
    {
        String schemaName = "test_basic_operations_schema_" + randomNameSuffix();
        String schemaLocation = locationPattern.locationForSchema(bucketName, schemaName);
        String tableName = "test_basic_operations_table_" + randomNameSuffix();
        String qualifiedTableName = schemaName + "." + tableName;
        String partitionQueryPart = (partitioned ? " WITH (partitioned_by = ARRAY['col_int'])" : "");

        String actualTableLocation;
        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaLocation + "')");
        try (UncheckedCloseable ignoredDropSchema = onClose("DROP SCHEMA " + schemaName)) {
            assertThat(getSchemaLocation(schemaName)).isEqualTo(schemaLocation);

            assertUpdate("CREATE TABLE " + qualifiedTableName + "(col_str varchar, col_int int)" + partitionQueryPart);
            try (UncheckedCloseable ignoredDropTable = onClose("DROP TABLE " + qualifiedTableName)) {
                String expectedTableLocation = (schemaLocation.endsWith("/") ? schemaLocation : schemaLocation + "/") + tableName;

                actualTableLocation = metastore.getTable(schemaName, tableName).orElseThrow().getStorage().getLocation();
                assertThat(actualTableLocation).isEqualTo(expectedTableLocation);

                assertUpdate("INSERT INTO " + qualifiedTableName + "  VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
                assertQuery("SELECT * FROM " + qualifiedTableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");

                assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
                validateDataFiles(partitioned ? "col_int" : "", qualifiedTableName, actualTableLocation);
            }
            assertThat(getTableFiles(actualTableLocation)).isEmpty();
        }
        validateFilesAfterDrop(actualTableLocation);
    }

    @Override
    public void testMergeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        // Row-level modifications are not supported for Hive tables
    }

    @Override
    protected void testOptimizeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        if (locationPattern == DOUBLE_SLASH || locationPattern == TRIPLE_SLASH || locationPattern == TWO_TRAILING_SLASHES) {
            assertThatThrownBy(() -> super.testOptimizeWithProvidedTableLocation(partitioned, locationPattern))
                    .hasMessageStartingWith("Unsupported location that cannot be internally represented: ")
                    .hasStackTraceContaining("SQL: CREATE TABLE test_optimize_");
            return;
        }
        super.testOptimizeWithProvidedTableLocation(partitioned, locationPattern);
    }

    @Test
    public void testAnalyzeWithProvidedTableLocation()
    {
        for (LocationPattern locationPattern : LocationPattern.values()) {
            testAnalyzeWithProvidedTableLocation(false, locationPattern);
            testAnalyzeWithProvidedTableLocation(true, locationPattern);
        }
    }

    private void testAnalyzeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        String location = locationPattern.locationForTable(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['col_int']" : "");

        String create = "CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (external_location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)";
        if (locationPattern == DOUBLE_SLASH || locationPattern == TRIPLE_SLASH || locationPattern == TWO_TRAILING_SLASHES) {
            assertQueryFails(create, "\\QUnsupported location that cannot be internally represented: " + location);
            return;
        }
        assertUpdate(create, 3);
        try (UncheckedCloseable ignored = onClose("DROP TABLE " + tableName)) {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

            // Check statistics collection on write
            if (partitioned) {
                assertQuery("SHOW STATS FOR " + tableName,
                        """
                        VALUES
                        ('col_str', 16.0, 1.0, 0.0, null, null, null),
                        ('col_int', null, 4.0, 0.0, null, 1, 4),
                        (null, null, null, null, 4.0, null, null)\
                        """);
            }
            else {
                assertQuery("SHOW STATS FOR " + tableName,
                        """
                        VALUES
                        ('col_str', 16.0, 3.0, 0.0, null, null, null),
                        ('col_int', null, 3.0, 0.0, null, 1, 4),
                        (null, null, null, null, 4.0, null, null)\
                        """);
            }

            // Check statistics collection explicitly
            assertUpdate("ANALYZE " + tableName, 4);

            if (partitioned) {
                assertQuery("SHOW STATS FOR " + tableName,
                        """
                        VALUES
                        ('col_str', 16.0, 1.0, 0.0, null, null, null),
                        ('col_int', null, 4.0, 0.0, null, 1, 4),
                        (null, null, null, null, 4.0, null, null)\
                        """);
            }
            else {
                assertQuery("SHOW STATS FOR " + tableName,
                        """
                        VALUES
                        ('col_str', 16.0, 4.0, 0.0, null, null, null),
                        ('col_int', null, 4.0, 0.0, null, 1, 4),
                        (null, null, null, null, 4.0, null, null)\
                        """);
            }
        }
    }

    @Test
    public void testPartitionProjectionWithProvidedTableLocation()
    {
        for (LocationPattern locationPattern : LocationPattern.values()) {
            if (locationPattern == DOUBLE_SLASH || locationPattern == TRIPLE_SLASH || locationPattern == TWO_TRAILING_SLASHES) {
                assertThatThrownBy(() -> testPartitionProjectionWithProvidedTableLocation(locationPattern))
                        .hasMessageStartingWith("Unsupported location that cannot be internally represented: ")
                        .hasStackTraceContaining("SQL: CREATE TABLE");
                continue;
            }
            testPartitionProjectionWithProvidedTableLocation(locationPattern);
        }
    }

    private void testPartitionProjectionWithProvidedTableLocation(LocationPattern locationPattern)
    {
        String tableName = "test_partition_projection_" + randomNameSuffix();
        String tableLocation = locationPattern.locationForTable(bucketName, schemaName, tableName);

        computeActual(format(
                """
                CREATE TABLE %s (
                name varchar(25),
                short_name varchar WITH (
                    partition_projection_type='date',
                    partition_projection_format='yyyy-MM-dd HH',
                    partition_projection_range=ARRAY['2001-01-22 00', '2001-01-22 06'],
                    partition_projection_interval=1,
                    partition_projection_interval_unit='HOURS'
                  )
                )
                WITH (
                  partitioned_by=ARRAY['short_name'],
                  partition_projection_enabled=true,
                  external_location = '%s'
                )
                """,
                tableName,
                tableLocation));

        assertUpdate("INSERT INTO " + tableName + " VALUES ('name1', '2001-01-22 00')", 1);

        assertQuery(format("SELECT name FROM %s", tableName), "VALUES ('name1')");
    }

    @Test
    public void testInvalidSchemaNameLocation()
    {
        String schemaNameSuffix = randomNameSuffix();
        String schemaName = "../test_create_schema_invalid_location_" + schemaNameSuffix;
        String tableName = "test_table_schema_invalid_location_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA \"%2$s\" WITH (location = 's3://%1$s/%2$s')".formatted(bucketName, schemaName));
        try (var _ = onClose("DROP SCHEMA \"" + schemaName + "\"")) {
            assertThat(query("CREATE TABLE \"" + schemaName + "\"." + tableName + " (col) AS VALUES 1"))
                    .failure().hasMessage("Error committing write to Hive")
                    .cause()
                    .cause().hasMessageMatching("Put failed for bucket \\[\\S+] key \\[\\.\\./test_create_schema_invalid_location_\\w+/test_table_schema_invalid_location_\\w+/\\S+]: .*")
                    // The message could be better. In AWS SDK v1 it used to be "Invalid URI".
                    .cause().hasMessageMatching("\\(Service: S3, Status Code: 400, Request ID: .*");
        }
    }

    @Test
    public void testSchemaNameEscape()
    {
        String schemaNameSuffix = randomNameSuffix();
        String schemaName = "../test_create_schema_escaped_" + schemaNameSuffix;
        String tableName = "test_table_schema_escaped_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA \"%2$s\"".formatted(bucketName, schemaName));
        try (var _ = onClose("DROP SCHEMA \"" + schemaName + "\"")) {
            assertUpdate("CREATE TABLE \"" + schemaName + "\"." + tableName + " (col) AS VALUES 1", 1);
            assertUpdate("DROP TABLE \"" + schemaName + "\"." + tableName);
        }
    }

    @Test
    public void testCreateFunction()
    {
        String name = "test_" + randomNameSuffix();
        String name2 = "test_" + randomNameSuffix();

        assertUpdate("CREATE FUNCTION " + name + "(x integer) RETURNS bigint COMMENT 't42' RETURN x * 42");

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQueryFails("SELECT " + name + "(2.9)", ".*Unexpected parameters.*");

        assertUpdate("CREATE FUNCTION " + name + "(x double) RETURNS double COMMENT 't88' RETURN x * 8.8");

        assertThat(query("SHOW FUNCTIONS"))
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(name, "bigint", "integer", "scalar", true, "t42")
                        .row(name, "double", "double", "scalar", true, "t88")
                        .build());

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");

        assertQueryFails("CREATE FUNCTION " + name + "(x int) RETURNS bigint RETURN x", "line 1:1: Function already exists");

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");

        assertUpdate("CREATE OR REPLACE FUNCTION " + name + "(x bigint) RETURNS bigint RETURN x * 23");
        assertUpdate("CREATE FUNCTION " + name2 + "(s varchar) RETURNS varchar RETURN 'Hello ' || s");

        assertThat(query("SHOW FUNCTIONS"))
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(name, "bigint", "integer", "scalar", true, "t42")
                        .row(name, "bigint", "bigint", "scalar", true, "")
                        .row(name, "double", "double", "scalar", true, "t88")
                        .row(name2, "varchar", "varchar", "scalar", true, "")
                        .build());

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(cast(99 as bigint))", "SELECT 2277");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");
        assertQuery("SELECT " + name2 + "('world')", "SELECT 'Hello world'");

        assertQueryFails("DROP FUNCTION " + name + "(varchar)", "line 1:1: Function not found");
        assertUpdate("DROP FUNCTION " + name + "(z bigint)");
        assertUpdate("DROP FUNCTION " + name + "(double)");
        assertUpdate("DROP FUNCTION " + name + "(int)");
        assertQueryFails("DROP FUNCTION " + name + "(bigint)", "line 1:1: Function not found");
        assertUpdate("DROP FUNCTION IF EXISTS " + name + "(bigint)");
        assertUpdate("DROP FUNCTION " + name2 + "(varchar)");
        assertQueryFails("DROP FUNCTION " + name2 + "(varchar)", "line 1:1: Function not found");
    }
}
