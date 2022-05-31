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
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.DataFileRecord.toDataFileRecord;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergTableWithCustomLocation
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

        return IcebergQueryRunner.builder()
                .setIcebergProperties(Map.of("iceberg.unique-table-location", "true"))
                .setMetastoreDirectory(metastoreDir)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testTableHasUuidSuffixInLocation()
    {
        String tableName = "table_with_uuid";
        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> table = metastore.getTable("tpch", tableName);
        assertThat(table).as("Table should exist").isPresent();
        String location = table.get().getStorage().getLocation();
        assertThat(location).matches(format(".*%s-[0-9a-f]{32}", tableName));
    }

    @Test
    public void testCreateAndDrop()
            throws IOException
    {
        String tableName = "test_create_and_drop";
        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Table table = metastore.getTable("tpch", tableName).orElseThrow();
        assertThat(table.getTableType()).isEqualTo(TableType.EXTERNAL_TABLE.name());

        Path tableLocation = new Path(table.getStorage().getLocation());
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, tableLocation);
        assertTrue(fileSystem.exists(tableLocation), "The directory corresponding to the table storage location should exist");
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_create_and_drop$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        DataFileRecord dataFile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));
        assertTrue(fileSystem.exists(new Path(dataFile.getFilePath())), "The data file should exist");
        assertQuerySucceeds(format("DROP TABLE %s", tableName));
        assertFalse(metastore.getTable("tpch", tableName).isPresent(), "Table should be dropped");
        assertFalse(fileSystem.exists(new Path(dataFile.getFilePath())), "The data file should have been removed");
        assertFalse(fileSystem.exists(tableLocation), "The directory corresponding to the dropped Iceberg table should not be removed because it may be shared with other tables");
    }

    @Test
    public void testCreateRenameDrop()
    {
        String tableName = "test_create_rename_drop";
        String renamedName = "test_create_rename_drop_renamed";
        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> table = metastore.getTable("tpch", tableName);
        assertThat(table).as("Table should exist").isPresent();
        String tableInitialLocation = table.get().getStorage().getLocation();

        assertQuerySucceeds(format("ALTER TABLE %s RENAME TO %s", tableName, renamedName));
        Optional<Table> renamedTable = metastore.getTable("tpch", renamedName);
        assertThat(renamedTable).as("Table should exist").isPresent();
        String renamedTableLocation = renamedTable.get().getStorage().getLocation();
        assertEquals(renamedTableLocation, tableInitialLocation, "Location should not be changed");

        assertQuerySucceeds(format("DROP TABLE %s", renamedName));
        assertThat(metastore.getTable("tpch", tableName)).as("Initial table should not exist").isEmpty();
        assertThat(metastore.getTable("tpch", renamedName)).as("Renamed table should be dropped").isEmpty();
    }

    @Test
    public void testCreateRenameCreate()
    {
        String tableName = "test_create_rename_create";
        String renamedName = "test_create_rename_create_renamed";
        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> table = metastore.getTable("tpch", tableName);
        assertThat(table).as("Table should exist").isPresent();
        String tableInitialLocation = table.get().getStorage().getLocation();

        assertQuerySucceeds(format("ALTER TABLE %s RENAME TO %s", tableName, renamedName));
        Optional<Table> renamedTable = metastore.getTable("tpch", renamedName);
        assertThat(renamedTable).as("Table should exist").isPresent();
        String renamedTableLocation = renamedTable.get().getStorage().getLocation();
        assertEquals(renamedTableLocation, tableInitialLocation, "Location should not be changed");

        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> recreatedTableWithInitialName = metastore.getTable("tpch", tableName);
        assertThat(recreatedTableWithInitialName).as("Table should exist").isPresent();
        String recreatedTableLocation = recreatedTableWithInitialName.get().getStorage().getLocation();
        assertNotEquals(tableInitialLocation, recreatedTableLocation, "Location should be different");
    }
}
