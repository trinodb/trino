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
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
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

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        metastoreDir = Files.createTempDirectory("test_iceberg").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);

        return createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("iceberg.unique-table-location", "true"),
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
    public void testTableHasUuidSuffixInLocation()
    {
        String tableName = "table_with_uuid";
        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> table = metastore.getTable(null, "tpch", tableName);
        assertTrue(table.isPresent(), "Table should exists");
        String location = table.get().getStorage().getLocation();
        assertThat(location).matches(format(".*%s-[0-9a-f]{32}", tableName));
    }

    @Test
    public void testCreateAndDrop()
    {
        String tableName = "test_create_and_drop";
        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> table = metastore.getTable(null, "tpch", tableName);
        assertTrue(table.isPresent(), "Table should exist");

        assertQuerySucceeds(format("DROP TABLE %s", tableName));
        assertFalse(metastore.getTable(null, "tpch", tableName).isPresent(), "Table should be dropped");
    }

    @Test
    public void testCreateRenameDrop()
    {
        String tableName = "test_create_rename_drop";
        String renamedName = "test_create_rename_drop_renamed";
        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> table = metastore.getTable(null, "tpch", tableName);
        assertTrue(table.isPresent(), "Table should exist");
        String tableInitialLocation = table.get().getStorage().getLocation();

        assertQuerySucceeds(format("ALTER TABLE %s RENAME TO %s", tableName, renamedName));
        Optional<Table> renamedTable = metastore.getTable(null, "tpch", renamedName);
        assertTrue(renamedTable.isPresent(), "Table should exist");
        String renamedTableLocation = renamedTable.get().getStorage().getLocation();
        assertEquals(renamedTableLocation, tableInitialLocation, "Location should not be changed");

        assertQuerySucceeds(format("DROP TABLE %s", renamedName));
        assertFalse(metastore.getTable(null, "tpch", tableName).isPresent(), "Initial table should not exists");
        assertFalse(metastore.getTable(null, "tpch", renamedName).isPresent(), "Renamed table should be dropped");
    }

    @Test
    public void testCreateRenameCreate()
    {
        String tableName = "test_create_rename_create";
        String renamedName = "test_create_rename_create_renamed";
        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> table = metastore.getTable(null, "tpch", tableName);
        assertTrue(table.isPresent(), "Table should exist");
        String tableInitialLocation = table.get().getStorage().getLocation();

        assertQuerySucceeds(format("ALTER TABLE %s RENAME TO %s", tableName, renamedName));
        Optional<Table> renamedTable = metastore.getTable(null, "tpch", renamedName);
        assertTrue(renamedTable.isPresent(), "Table should exist");
        String renamedTableLocation = renamedTable.get().getStorage().getLocation();
        assertEquals(renamedTableLocation, tableInitialLocation, "Location should not be changed");

        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> recreatedTableWithInitialName = metastore.getTable(null, "tpch", tableName);
        assertTrue(recreatedTableWithInitialName.isPresent(), "Table should exist");
        String recreatedTableLocation = recreatedTableWithInitialName.get().getStorage().getLocation();
        assertNotEquals(tableInitialLocation, recreatedTableLocation, "Location should be different");
    }
}
