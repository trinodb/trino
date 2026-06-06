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
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.hive.BaseS3AndGlueTest;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.trino.plugin.iceberg.IcebergTestUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseIcebergS3AndGlueTest
        extends BaseS3AndGlueTest
{
    protected BaseIcebergS3AndGlueTest(String bucketName)
    {
        super("partitioning", "location", bucketName);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", schemaPath())
                        .put("fs.s3.enabled", "true")
                        .putAll(s3AndGlueProperties())
                        .buildOrThrow())
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(schemaName)
                        .withSchemaProperties(Map.of("location", "'" + schemaPath() + "'"))
                        .build())
                .build();
        metastore = getConnectorService(queryRunner, GlueHiveMetastore.class);
        return queryRunner;
    }

    protected Map<String, String> s3AndGlueProperties()
    {
        return Map.of();
    }

    @Override
    protected void validateDataFiles(String partitionColumn, String tableName, String location)
    {
        getActiveFiles(tableName).forEach(dataFile -> {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            String partitionPart = partitionColumn.isEmpty() ? "" : partitionColumn + "=[a-z0-9]+/";
            assertThat(dataFile).matches("^" + Pattern.quote(locationDirectory) + "data/" + partitionPart + "[a-zA-Z0-9_-]+.(orc|parquet)$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        getAllMetadataDataFilesFromTableDirectory(location).forEach(metadataFile -> {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            assertThat(metadataFile).matches("^" + Pattern.quote(locationDirectory) + "metadata/[a-zA-Z0-9_-]+.(avro|metadata.json|stats)$");
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
        String partitionQueryPart = (partitioned ? ",partitioning = ARRAY['col_str']" : "");

        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
        try (UncheckedCloseable ignored = onClose("DROP TABLE " + tableName)) {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

            String expectedStatistics =
                    """
                    VALUES
                    ('col_str', %s, 4.0, 0.0, null, null, null),
                    ('col_int', null, 4.0, 0.0, null, 1, 4),
                    (null, null, null, null, 4.0, null, null)"""
                            .formatted(partitioned ? "442.0" : "256.0");

            // Check extended statistics collection on write
            assertQuery("SHOW STATS FOR " + tableName, expectedStatistics);

            // drop stats
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");
            // Check extended statistics collection explicitly
            assertUpdate("ANALYZE " + tableName);
            assertQuery("SHOW STATS FOR " + tableName, expectedStatistics);
        }
    }
}
