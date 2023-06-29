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
package io.trino.plugin.deltalake.metastore.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.hive.BaseS3AndGlueMetastoreTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaS3AndGlueMetastoreTest
        extends BaseS3AndGlueMetastoreTest
{
    public TestDeltaS3AndGlueMetastoreTest()
    {
        super("partitioned_by", "location", requireNonNull(System.getenv("S3_BUCKET"), "Environment variable not set: S3_BUCKET"));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        metastore = createTestingGlueHiveMetastore(Path.of(schemaPath()));
        DistributedQueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .setCatalogName(DELTA_CATALOG)
                .setDeltaProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", schemaPath())
                        .put("delta.enable-non-concurrent-writes", "true")
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
            assertThat(dataFile).matches("^" + locationDirectory + partitionPart + "[a-zA-Z0-9_-]+$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        String locationDirectory = location.endsWith("/") ? location : location + "/";
        getAllMetadataDataFilesFromTableDirectory(location).forEach(metadataFile ->
        {
            assertThat(metadataFile).matches("^" + locationDirectory + "_delta_log/[0-9]+.json$");
            verifyPathExist(metadataFile);
        });

        assertThat(getExtendedStatisticsFileFromTableDirectory(location)).matches("^" + locationDirectory + "_delta_log/_trino_meta/extended_stats.json$");
    }

    @Override
    protected void validateFilesAfterDrop(String location)
    {
        // In Delta table created with location in treated as external, so files are not removed
        assertThat(getTableFiles(location)).isNotEmpty();
    }

    @Override
    protected Set<String> getAllDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> !path.contains("_delta_log"))
                .collect(Collectors.toUnmodifiableSet());
    }

    private Set<String> getAllMetadataDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/metadata"))
                .collect(Collectors.toUnmodifiableSet());
    }

    private String getExtendedStatisticsFileFromTableDirectory(String tableLocation)
    {
        return getOnlyElement(getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/_trino_meta"))
                .collect(Collectors.toUnmodifiableSet()));
    }
}
