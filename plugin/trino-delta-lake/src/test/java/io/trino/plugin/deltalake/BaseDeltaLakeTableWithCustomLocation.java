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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseDeltaLakeTableWithCustomLocation
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_tables_with_custom_location" + randomNameSuffix();
    protected static final String CATALOG_NAME = "delta_with_custom_location";
    protected File metastoreDir;
    protected HiveMetastore metastore;

    @Test
    public void testTableHasUuidSuffixInLocation()
    {
        String tableName = "table_with_uuid" + randomNameSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as val", tableName));
        Optional<Table> table = metastore.getTable(SCHEMA, tableName);
        assertTrue(table.isPresent(), "Table should exists");
        String location = table.get().getStorage().getLocation();
        assertThat(location).matches(format(".*%s-[0-9a-f]{32}", tableName));
    }

    @Test
    public void testCreateAndDrop()
            throws IOException
    {
        String tableName = "test_create_and_drop" + randomNameSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as val", tableName));
        Table table = metastore.getTable(SCHEMA, tableName).orElseThrow();
        assertThat(table.getTableType()).isEqualTo(MANAGED_TABLE.name());

        String tableLocation = table.getStorage().getLocation();
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(getSession().toConnectorSession());
        assertTrue(fileSystem.listFiles(tableLocation).hasNext(), "The directory corresponding to the table storage location should exist");
        List<MaterializedRow> materializedRows = computeActual("SELECT \"$path\" FROM " + tableName).getMaterializedRows();
        assertEquals(materializedRows.size(), 1);
        String filePath = (String) materializedRows.get(0).getField(0);
        assertTrue(fileSystem.listFiles(filePath).hasNext(), "The data file should exist");
        assertQuerySucceeds(format("DROP TABLE %s", tableName));
        assertFalse(metastore.getTable(SCHEMA, tableName).isPresent(), "Table should be dropped");
        assertFalse(fileSystem.listFiles(filePath).hasNext(), "The data file should have been removed");
        assertFalse(fileSystem.listFiles(tableLocation).hasNext(), "The directory corresponding to the dropped Delta Lake table should be removed");
    }
}
