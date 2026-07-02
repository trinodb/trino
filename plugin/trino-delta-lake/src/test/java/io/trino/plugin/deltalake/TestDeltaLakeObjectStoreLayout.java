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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeltaLakeObjectStoreLayout
        extends AbstractTestQueryFramework
{
    private static final String OBJECT_STORE_LAYOUT_ENABLED_CATALOG = "delta_object_store_layout_enabled";

    private Path temporaryDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        temporaryDirectory = Files.createTempDirectory("delta-object-store-layout");
        closeAfterClass(() -> deleteRecursively(temporaryDirectory, ALLOW_INSECURE));

        String metastoreDirectory = temporaryDirectory.resolve("metastore").toUri().toString();
        QueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addDeltaProperty("hive.metastore.catalog.dir", metastoreDirectory)
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.object-store-layout.enabled", "false")
                .build();
        queryRunner.createCatalog(OBJECT_STORE_LAYOUT_ENABLED_CATALOG, CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", metastoreDirectory)
                .put("fs.hadoop.enabled", "true")
                .put("delta.enable-non-concurrent-writes", "true")
                .put("delta.object-store-layout.enabled", "true")
                .buildOrThrow());
        return queryRunner;
    }

    @Test
    void testDisabledUsesFlatUnpartitionedPaths()
    {
        String tableName = "test_binary_hash_disabled_unpartitioned_" + randomNameSuffix();
        Path tableLocation = temporaryDirectory.resolve("data").resolve(tableName);

        assertUpdate(
                "CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS value"
                        .formatted(tableName, tableLocation.toUri()),
                1);
        try {
            String dataFilePath = (String) computeActual("SELECT \"$path\" FROM " + tableName).getOnlyValue();
            assertThat(Path.of(URI.create(dataFilePath)).getParent()).isEqualTo(tableLocation);
            assertQuery("SELECT value FROM \"%s$properties\" WHERE key = 'delta.randomizeFilePrefixes'".formatted(tableName), "VALUES 'false'");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testDisabledUsesLegacyDataAndChangeDataPaths()
            throws IOException
    {
        String tableName = "test_binary_hash_disabled_" + randomNameSuffix();
        Path tableLocation = temporaryDirectory.resolve("data").resolve(tableName);

        assertUpdate(
                ("CREATE TABLE %s WITH (location = '%s', change_data_feed_enabled = true, partitioned_by = ARRAY['part']) " +
                        "AS SELECT * FROM (VALUES (1, 'first', 'one'), (2, 'second', 'one'), (3, 'third', 'two')) t(id, name, part)")
                        .formatted(tableName, tableLocation.toUri()),
                3);
        try {
            assertLegacyDataPaths(tableName);
            assertQuery("SELECT value FROM \"%s$properties\" WHERE key = 'delta.randomizeFilePrefixes'".formatted(tableName), "VALUES 'false'");

            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'fourth', 'one'), (5, 'fifth', 'two')", 2);
            assertLegacyDataPaths(tableName);

            assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
            assertLegacyDataPaths(tableName);

            assertUpdate("MERGE INTO " + tableName + " target USING (VALUES 1) source(id) ON target.id = source.id " +
                    "WHEN MATCHED THEN UPDATE SET name = 'updated'", 1);
            assertLegacyDataPaths(tableName);
            assertQuery("SELECT id, name, part FROM " + tableName, "VALUES " +
                    "(1, 'updated', 'one'), (2, 'second', 'one'), (3, 'third', 'two'), (4, 'fourth', 'one'), (5, 'fifth', 'two')");

            List<Path> changeDataFiles;
            try (Stream<Path> files = Files.walk(tableLocation.resolve(DeltaLakeCdfPageSink.CHANGE_DATA_FOLDER_NAME))) {
                changeDataFiles = files
                        .filter(Files::isRegularFile)
                        .filter(path -> !path.getFileName().toString().endsWith(".crc"))
                        .collect(toImmutableList());
            }
            assertThat(changeDataFiles)
                    .isNotEmpty()
                    .allSatisfy(path -> assertThat(tableLocation.resolve(DeltaLakeCdfPageSink.CHANGE_DATA_FOLDER_NAME).relativize(path).toString())
                            .matches("part=(one|two)/[^/]+"));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testEnabledTableMetadataControlsRewriteWhenCatalogDisabled()
    {
        String tableName = "test_binary_hash_disabled_rewrite_" + randomNameSuffix();
        String enabledTableName = OBJECT_STORE_LAYOUT_ENABLED_CATALOG + ".tpch." + tableName;
        Path tableLocation = temporaryDirectory.resolve("data").resolve(tableName);
        Session singleWriterSession = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                .build();

        assertUpdate(
                singleWriterSession,
                ("CREATE TABLE %s WITH (location = '%s', partitioned_by = ARRAY['part']) " +
                        "AS SELECT * FROM (VALUES (1, 'first', 'one'), (2, 'second', 'one')) t(id, name, part)")
                        .formatted(enabledTableName, tableLocation.toUri()),
                2);
        try {
            assertBinaryDataPaths(enabledTableName);
            assertQuery("SELECT value FROM %s.tpch.\"%s$properties\" WHERE key = 'delta.randomizeFilePrefixes'"
                    .formatted(OBJECT_STORE_LAYOUT_ENABLED_CATALOG, tableName), "VALUES 'true'");

            assertUpdate(singleWriterSession, "MERGE INTO " + tableName + " target USING (VALUES 1) source(id) ON target.id = source.id " +
                    "WHEN MATCHED THEN UPDATE SET name = 'updated'", 1);

            assertBinaryDataPaths(tableName);
            assertQuery("SELECT id, name, part FROM " + tableName, "VALUES (1, 'updated', 'one'), (2, 'second', 'one')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testDisabledTableMetadataControlsInsertWhenCatalogEnabled()
    {
        String tableName = "test_disabled_metadata_" + randomNameSuffix();
        String enabledTableName = OBJECT_STORE_LAYOUT_ENABLED_CATALOG + ".tpch." + tableName;
        Path tableLocation = temporaryDirectory.resolve("data").resolve(tableName);

        assertUpdate(
                ("CREATE TABLE %s WITH (location = '%s', partitioned_by = ARRAY['part']) " +
                        "AS SELECT * FROM (VALUES (1, 'first', 'one'), (2, 'second', 'two')) t(id, name, part)")
                        .formatted(tableName, tableLocation.toUri()),
                2);
        try {
            assertLegacyDataPaths(tableName);

            assertUpdate("INSERT INTO " + enabledTableName + " VALUES (3, 'third', 'one')", 1);

            assertLegacyDataPaths(tableName);
            assertQuery("SELECT id, name, part FROM " + tableName, "VALUES (1, 'first', 'one'), (2, 'second', 'two'), (3, 'third', 'one')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testObjectStoreLayoutTablePropertyControlsSubsequentWrites()
    {
        String tableName = "test_object_store_layout_property_" + randomNameSuffix();
        Path tableLocation = temporaryDirectory.resolve("data").resolve(tableName);

        assertUpdate(
                ("CREATE TABLE %s WITH (location = '%s', object_store_layout_enabled = true, partitioned_by = ARRAY['part']) " +
                        "AS SELECT * FROM (VALUES (1, 'first', 'one')) t(id, name, part)")
                        .formatted(tableName, tableLocation.toUri()),
                1);
        try {
            assertBinaryDataPaths(tableName);
            assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                    .contains("object_store_layout_enabled = true");

            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES object_store_layout_enabled = false");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                    .contains("object_store_layout_enabled = false");
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'second', 'two')", 1);
            assertThat((String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 2"))
                    .matches(".*/part=two/[^/]+");

            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES object_store_layout_enabled = true");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                    .contains("object_store_layout_enabled = true");
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'third', 'three')", 1);
            assertThat((String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 3"))
                    .matches(".*/[01]{4}/[01]{4}/[01]{4}/[01]{8}/[^/]+");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testObjectStoreLayoutTablePropertyForCreateTable()
    {
        String tableName = "test_object_store_layout_create_" + randomNameSuffix();
        Path tableLocation = temporaryDirectory.resolve("data").resolve(tableName);

        assertUpdate(("CREATE TABLE %s (id integer, part varchar) " +
                "WITH (location = '%s', object_store_layout_enabled = true, partitioned_by = ARRAY['part'])")
                .formatted(tableName, tableLocation.toUri()));
        try {
            assertQuery("SELECT value FROM \"%s$properties\" WHERE key = 'delta.randomizeFilePrefixes'".formatted(tableName), "VALUES 'true'");

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            assertBinaryDataPaths(tableName);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private void assertBinaryDataPaths(String tableName)
    {
        assertThat(computeActual("SELECT DISTINCT \"$path\" FROM " + tableName).getOnlyColumnAsSet())
                .isNotEmpty()
                .allSatisfy(path -> assertThat((String) path).matches(".*/[01]{4}/[01]{4}/[01]{4}/[01]{8}/[^/]+"));
    }

    private void assertLegacyDataPaths(String tableName)
    {
        assertThat(computeActual("SELECT DISTINCT \"$path\" FROM " + tableName).getOnlyColumnAsSet())
                .isNotEmpty()
                .allSatisfy(path -> assertThat((String) path).matches(".*/part=(one|two)/[^/]+"));
    }
}
