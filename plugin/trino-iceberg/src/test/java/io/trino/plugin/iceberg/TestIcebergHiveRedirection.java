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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.HivePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // Uses file metastore sharing location between catalogs
public class TestIcebergHiveRedirection
        extends AbstractTestQueryFramework
{
    private final String hiveCatalog = HIVE_CATALOG;
    private final String icebergCatalog = ICEBERG_CATALOG;
    private final String schema = "default";

    private final Session icebergSession = testSessionBuilder()
            .setCatalog(icebergCatalog)
            .setSchema(schema)
            .build();

    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession)
                .build();

        File dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve(getClass().getSimpleName()).toFile();
        verify(dataDirectory.mkdirs());

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog(
                icebergCatalog,
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        // Intentionally sharing the file metastore directory with Hive
                        "hive.metastore.catalog.dir", dataDirectory.getPath(),
                        "fs.hadoop.enabled", "true"));

        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog(
                hiveCatalog,
                "hive",
                ImmutableMap.of(
                        "hive.iceberg-catalog-name", icebergCatalog,
                        "hive.metastore", "file",
                        // Intentionally sharing the file metastore directory with Iceberg
                        "hive.metastore.catalog.dir", dataDirectory.getPath(),
                        "fs.hadoop.enabled", "true"));

        queryRunner.execute("CREATE SCHEMA " + schema);

        return queryRunner;
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @Test
    public void testDropCorruptedTableWithHiveRedirection()
            throws Exception
    {
        String tableName = "test_drop_corrupted_table_with_hive_redirection_" + randomNameSuffix();
        String hiveTableName = "%s.%s.%s".formatted(hiveCatalog, schema, tableName);
        String icebergTableName = "%s.%s.%s".formatted(icebergCatalog, schema, tableName);

        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE TABLE " + icebergTableName + " (id INT, country VARCHAR, independence ROW(month VARCHAR, year INT))");
        queryRunner.execute("INSERT INTO " + icebergTableName + " VALUES (1, 'INDIA', ROW ('Aug', 1947)), (2, 'POLAND', ROW ('Nov', 1918)), (3, 'USA', ROW ('Jul', 1776))");

        assertThat(queryRunner.execute("TABLE " + hiveTableName))
                .containsAll(queryRunner.execute("TABLE " + icebergTableName));

        Location tableLocation = Location.of((String) queryRunner.execute("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*/[^/]*$', '') FROM " + tableName).getOnlyValue());
        Location metadataLocation = tableLocation.appendPath("metadata");

        // break the table by deleting all metadata files
        fileSystem.deleteDirectory(metadataLocation);
        assertThat(fileSystem.listFiles(metadataLocation).hasNext())
                .describedAs("Metadata location should not exist")
                .isFalse();

        // DROP TABLE should succeed using hive redirection
        queryRunner.execute("DROP TABLE " + hiveTableName);
        assertThat(queryRunner.tableExists(getSession(), icebergTableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }
}
