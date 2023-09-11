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
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.hive.BaseS3AndGlueMetastoreTest.LocationPattern.DOUBLE_SLASH;
import static io.trino.plugin.hive.BaseS3AndGlueMetastoreTest.LocationPattern.TRIPLE_SLASH;
import static io.trino.plugin.hive.BaseS3AndGlueMetastoreTest.LocationPattern.TWO_TRAILING_SLASHES;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveS3AndGlueMetastoreTest
        extends BaseS3AndGlueMetastoreTest
{
    public TestHiveS3AndGlueMetastoreTest()
    {
        super("partitioned_by", "external_location", requireNonNull(System.getenv("S3_BUCKET"), "Environment S3_BUCKET was not set"));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        metastore = createTestingGlueHiveMetastore(Path.of(schemaPath()));

        Session session = createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin"))));
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(session)
                .setCreateTpchSchemas(false)
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperty("hive.non-managed-table-writes-enabled", "true")
                .setMetastore(runner -> metastore)
                .build();
        queryRunner.execute("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaPath() + "')");
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
            assertThat(dataFile).matches("^" + locationDirectory + partitionPart + "[a-zA-Z0-9_-]+$");
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
    @Test(dataProvider = "locationPatternsDataProvider")
    public void testBasicOperationsWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
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

    @Test(dataProvider = "locationPatternsDataProvider")
    public void testBasicOperationsWithProvidedTableLocationNonCTAS(boolean partitioned, LocationPattern locationPattern)
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
    @Test(dataProvider = "locationPatternsDataProvider")
    public void testBasicOperationsWithProvidedSchemaLocation(boolean partitioned, LocationPattern locationPattern)
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
                String expectedTableLocation = ((schemaLocation.endsWith("/") ? schemaLocation : schemaLocation + "/") + tableName)
                        // Hive normalizes repeated slashes
                        .replaceAll("(?<!(s3:))/+", "/");

                actualTableLocation = metastore.getTable(schemaName, tableName).orElseThrow().getStorage().getLocation();
                assertThat(actualTableLocation).matches(expectedTableLocation);

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
    @Test(dataProvider = "locationPatternsDataProvider")
    public void testMergeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        // Row-level modifications are not supported for Hive tables
    }

    @Override
    public void testOptimizeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        if (locationPattern == DOUBLE_SLASH || locationPattern == TRIPLE_SLASH || locationPattern == TWO_TRAILING_SLASHES) {
            assertThatThrownBy(() -> super.testOptimizeWithProvidedTableLocation(partitioned, locationPattern))
                    .hasMessageStartingWith("Unsupported location that cannot be internally represented: ")
                    .hasStackTraceContaining("SQL: CREATE TABLE test_optimize_");
            return;
        }
        super.testOptimizeWithProvidedTableLocation(partitioned, locationPattern);
    }

    @Test(dataProvider = "locationPatternsDataProvider")
    public void testAnalyzeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
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
                assertQuery("SHOW STATS FOR " + tableName, """
                        VALUES
                        ('col_str', 0.0, 1.0, 0.0, null, null, null),
                        ('col_int', null, 4.0, 0.0, null, 1, 4),
                        (null, null, null, null, 4.0, null, null)""");
            }
            else {
                assertQuery("SHOW STATS FOR " + tableName, """
                        VALUES
                        ('col_str', 16.0, 3.0, 0.0, null, null, null),
                        ('col_int', null, 3.0, 0.0, null, 1, 4),
                        (null, null, null, null, 4.0, null, null)""");
            }

            // Check statistics collection explicitly
            assertUpdate("ANALYZE " + tableName, 4);

            if (partitioned) {
                assertQuery("SHOW STATS FOR " + tableName, """
                        VALUES
                        ('col_str', 16.0, 1.0, 0.0, null, null, null),
                        ('col_int', null, 4.0, 0.0, null, 1, 4),
                        (null, null, null, null, 4.0, null, null)""");
            }
            else {
                assertQuery("SHOW STATS FOR " + tableName, """
                        VALUES
                        ('col_str', 16.0, 4.0, 0.0, null, null, null),
                        ('col_int', null, 4.0, 0.0, null, 1, 4),
                        (null, null, null, null, 4.0, null, null)""");
            }
        }
    }

    @Test
    public void testCreateTableWithIncorrectLocation()
    {
        String tableName = "test_create_table_with_incorrect_location_" + randomNameSuffix();
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + "(col_str varchar, col_int integer) WITH (external_location = '" + location + "')"))
                .hasMessageContaining("External location is not a valid file system URI")
                .hasStackTraceContaining("Fragment is not allowed in a file system location");
    }

    @Test
    public void testCtasWithIncorrectLocation()
    {
        String tableName = "test_ctas_with_incorrect_location_" + randomNameSuffix();
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                " WITH (external_location = '" + location + "')" +
                " AS VALUES ('str1', 1)"))
                .hasMessageContaining("External location is not a valid file system URI")
                .hasStackTraceContaining("Fragment is not allowed in a file system location");
    }

    @Test
    public void testSchemaNameEscape()
    {
        String schemaNameSuffix = randomNameSuffix();
        String schemaName = "../test_create_schema_escaped_" + schemaNameSuffix;
        String tableName = "test_table_schema_escaped_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA \"%2$s\" WITH (location = 's3://%1$s/%2$s')".formatted(bucketName, schemaName));
        try (UncheckedCloseable ignored = onClose("DROP SCHEMA \"" + schemaName + "\"")) {
            assertQueryFails("CREATE TABLE \"" + schemaName + "\"." + tableName + " (col) AS VALUES 1", "Failed checking path: .*");
        }
    }
}
