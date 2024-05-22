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
package io.trino.plugin.hive.metastore.file;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.AbstractTestHiveMetastore;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFileHiveMetastore
        extends AbstractTestHiveMetastore
{
    private final Path tempDir;
    private final HiveMetastore metastore;

    TestFileHiveMetastore()
            throws IOException
    {
        tempDir = createTempDirectory("test");
        LocalFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(tempDir);

        metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                fileSystemFactory,
                false,
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory("local:///")
                        .setMetastoreUser("test")
                        .setDisableLocationChecks(true));
    }

    @AfterAll
    void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Override
    protected HiveMetastore getMetastore()
    {
        return metastore;
    }

    @Test
    public void testPreserveHudiInputFormat()
    {
        String databaseName = "test_database_" + randomNameSuffix();
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, "query_id"))
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        getMetastore().createDatabase(database.build());

        StorageFormat storageFormat = StorageFormat.create(PARQUET_HIVE_SERDE_CLASS, HUDI_PARQUET_INPUT_FORMAT, MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        String tableName = "some_table_name" + randomNameSuffix();
        Table table = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .addDataColumn(new Column("foo", HIVE_INT, Optional.empty(), Map.of()))
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat)
                        .setLocation("file:///dev/null"))
                .build();

        metastore.createTable(table, NO_PRIVILEGES);

        Table saved = metastore.getTable(databaseName, tableName).orElseThrow();

        assertThat(saved.getStorage())
                .isEqualTo(table.getStorage());

        metastore.dropTable(databaseName, tableName, false);

        getMetastore().dropDatabase(databaseName, false);
    }
}
