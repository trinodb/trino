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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.BaseS3AndGlueMetastoreTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergS3AndGlueMetastoreTest
        extends BaseS3AndGlueMetastoreTest
{
    public TestIcebergS3AndGlueMetastoreTest()
    {
        super("partitioning", "location", requireNonNull(System.getenv("S3_BUCKET"), "Environment S3_BUCKET was not set"));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        metastore = createTestingGlueHiveMetastore(Path.of(schemaPath()));
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", schemaPath())
                        .buildOrThrow())
                .build();
        queryRunner.execute("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaPath() + "')");
        return queryRunner;
    }

    @Override
    protected void validateDataFiles(String partitionColumn, String tableName, String location)
    {
        getActiveFiles(tableName).forEach(dataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            String partitionPart = partitionColumn.isEmpty() ? "" : partitionColumn + "=[a-z0-9]+/";
            assertThat(dataFile).matches("^" + locationDirectory + "data/" + partitionPart + "[a-zA-Z0-9_-]+.(orc|parquet)$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        getAllMetadataDataFilesFromTableDirectory(location).forEach(metadataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            assertThat(metadataFile).matches("^" + locationDirectory + "metadata/[a-zA-Z0-9_-]+.(avro|metadata.json|stats)$");
            verifyPathExist(metadataFile);
        });
    }

    @Override
    protected String validateTableLocation(String tableName, String expectedLocation)
    {
        // Iceberg removes trailing slashes from location, and it's expected.
        if (expectedLocation.endsWith("/")) {
            expectedLocation = expectedLocation.replaceFirst("/+$", "");
        }
        String actualTableLocation = getTableLocation(tableName);
        assertThat(actualTableLocation).isEqualTo(expectedLocation);
        return actualTableLocation;
    }

    private Set<String> getAllMetadataDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/metadata"))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    protected Set<String> getAllDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/data"))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Test(dataProvider = "locationPatternsDataProvider")
    public void testAnalyzeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        String location = locationPattern.locationForTable(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioning = ARRAY['col_str']" : "");

        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
        try (UncheckedCloseable ignored = onClose("DROP TABLE " + tableName)) {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

            String expectedStatistics = """
                    VALUES
                    ('col_str', %s, 4.0, 0.0, null, null, null),
                    ('col_int', null, 4.0, 0.0, null, 1, 4),
                    (null, null, null, null, 4.0, null, null)"""
                    .formatted(partitioned ? "475.0" : "264.0");

            // Check extended statistics collection on write
            assertQuery("SHOW STATS FOR " + tableName, expectedStatistics);

            // drop stats
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");
            // Check extended statistics collection explicitly
            assertUpdate("ANALYZE " + tableName);
            assertQuery("SHOW STATS FOR " + tableName, expectedStatistics);
        }
    }

    @Test
    public void testCreateTableWithIncorrectLocation()
    {
        String tableName = "test_create_table_with_incorrect_location_" + randomNameSuffix();
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) WITH (location = '" + location + "')"))
                .hasMessageContaining("Fragment is not allowed in a file system location");
    }

    @Test
    public void testCtasWithIncorrectLocation()
    {
        String tableName = "test_create_table_with_incorrect_location_" + randomNameSuffix();
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName +
                " WITH (location = '" + location + "')" +
                " AS SELECT * FROM tpch.tiny.nation"))
                .hasMessageContaining("Fragment is not allowed in a file system location");
    }
}
