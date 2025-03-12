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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Table;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseDeltaLakeTableWithCustomLocation
        extends AbstractTestQueryFramework
{
    @Test
    public void testTableHasUuidSuffixInLocation()
    {
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "table_with_uuid" + randomNameSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as val", tableName));
        Optional<Table> table = metastore().getTable(schema, tableName);
        assertThat(table.isPresent())
                .describedAs("Table should exists")
                .isTrue();
        String location = table.get().getStorage().getLocation();
        assertThat(location).matches(format(".*%s-[0-9a-f]{32}", tableName));
    }

    @Test
    public void testCreateAndDrop()
            throws IOException
    {
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_create_and_drop" + randomNameSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as val", tableName));
        Table table = metastore().getTable(schema, tableName).orElseThrow();
        assertThat(table.getTableType()).isEqualTo(MANAGED_TABLE.name());

        Location tableLocation = Location.of(table.getStorage().getLocation());
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(getSession().toConnectorSession());
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("The directory corresponding to the table storage location should exist")
                .isTrue();
        List<MaterializedRow> materializedRows = computeActual("SELECT \"$path\" FROM " + tableName).getMaterializedRows();
        assertThat(materializedRows).hasSize(1);
        Location filePath = Location.of((String) materializedRows.get(0).getField(0));
        assertThat(fileSystem.listFiles(filePath).hasNext())
                .describedAs("The data file should exist")
                .isTrue();
        assertQuerySucceeds(format("DROP TABLE %s", tableName));
        assertThat(metastore().getTable(schema, tableName).isPresent())
                .describedAs("Table should be dropped")
                .isFalse();
        assertThat(fileSystem.listFiles(filePath).hasNext())
                .describedAs("The data file should have been removed")
                .isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("The directory corresponding to the dropped Delta Lake table should be removed")
                .isFalse();
    }

    protected HiveMetastore metastore()
    {
        return TestingDeltaLakeUtils.getConnectorService(getQueryRunner(), HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
    }
}
