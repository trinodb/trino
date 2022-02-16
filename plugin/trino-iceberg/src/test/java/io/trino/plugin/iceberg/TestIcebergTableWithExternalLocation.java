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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.DataFileRecord.toDataFileRecord;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergTableWithExternalLocation
        extends AbstractTestQueryFramework
{
    private FileHiveMetastore metastore;
    private File metastoreDir;
    private HdfsEnvironment hdfsEnvironment;
    private HdfsContext hdfsContext;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        metastoreDir = Files.createTempDirectory("test_iceberg").toFile();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        FileHiveMetastoreConfig config = new FileHiveMetastoreConfig()
                .setCatalogDirectory(metastoreDir.toURI().toString())
                .setMetastoreUser("test");
        hdfsContext = new HdfsContext(ConnectorIdentity.ofUser(config.getMetastoreUser()));
        metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                hdfsEnvironment,
                new MetastoreConfig(),
                config);

        return createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(),
                Optional.of(metastoreDir));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCreateAndDrop()
            throws IOException
    {
        String tableName = "test_table_external_create_and_drop";
        File tempDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().toFile();
        String tempDirPath = tempDir.toURI().toASCIIString() + randomTableSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s ( x bigint) WITH (location = '%s')", tableName, tempDirPath));
        assertQuerySucceeds(format("INSERT INTO %s VALUES (1), (2), (3)", tableName));

        Table table = metastore.getTable("tpch", tableName).orElseThrow();
        assertThat(table.getTableType()).isEqualTo(TableType.EXTERNAL_TABLE.name());
        Path tableLocation = new Path(table.getStorage().getLocation());
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, tableLocation);
        assertTrue(fileSystem.exists(tableLocation), "The directory corresponding to the table storage location should exist");
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_table_external_create_and_drop$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        DataFileRecord dataFile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));
        assertTrue(fileSystem.exists(new Path(dataFile.getFilePath())), "The data file should exist");

        assertQuerySucceeds(format("DROP TABLE %s", tableName));
        assertFalse(metastore.getTable("tpch", tableName).isPresent(), "Table should be dropped");
        assertFalse(fileSystem.exists(new Path(dataFile.getFilePath())), "The data file should have been removed");
        assertFalse(fileSystem.exists(tableLocation), "The directory corresponding to the dropped Iceberg table should not be removed because it may be shared with other tables");
    }
}
