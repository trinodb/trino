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
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.util.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFileHiveMetastore
{
    private Path tmpDir;
    private FileHiveMetastore metastore;

    @BeforeClass
    public void setUp()
            throws IOException
    {
        tmpDir = createTempDirectory(getClass().getSimpleName());

        metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                HDFS_FILE_SYSTEM_FACTORY,
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(tmpDir.toString())
                        .setDisableLocationChecks(true)
                /*.setMetastoreUser("test")*/);

        metastore.createDatabase(Database.builder()
                .setDatabaseName("default")
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(USER))
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tmpDir, ALLOW_INSECURE);
        metastore = null;
        tmpDir = null;
    }

    @Test
    public void testPreserveHudiInputFormat()
    {
        StorageFormat storageFormat = StorageFormat.create(
                ParquetHiveSerDe.class.getName(),
                HUDI_PARQUET_INPUT_FORMAT,
                MapredParquetOutputFormat.class.getName());

        Table table = Table.builder()
                .setDatabaseName("default")
                .setTableName("some_table_name" + randomNameSuffix())
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .addDataColumn(new Column("foo", HIVE_INT, Optional.empty()))
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat)
                        .setLocation("file:///dev/null"))
                .build();

        metastore.createTable(table, NO_PRIVILEGES);

        Table saved = metastore.getTable(table.getDatabaseName(), table.getTableName()).orElseThrow();

        assertThat(saved.getStorage())
                .isEqualTo(table.getStorage());

        metastore.dropTable(table.getDatabaseName(), table.getTableName(), false);
    }
}
