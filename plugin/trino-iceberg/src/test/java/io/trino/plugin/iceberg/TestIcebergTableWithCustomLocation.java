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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.iceberg.DataFileRecord.toDataFileRecord;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergTableWithCustomLocation
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(Map.of("iceberg.unique-table-location", "true"))
                .build();

        metastore = getHiveMetastore(queryRunner);

        return queryRunner;
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
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
        assertThat(table.getTableType()).isEqualTo(EXTERNAL_TABLE.name());

        Location tableLocation = Location.of(table.getStorage().getLocation());
        assertThat(fileSystem.newInputFile(tableLocation).exists())
                .describedAs("The directory corresponding to the table storage location should exist")
                .isTrue();

        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_create_and_drop$files\"");
        assertThat(materializedResult.getRowCount()).isEqualTo(1);
        DataFileRecord dataFile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));
        Location dataFileLocation = Location.of(dataFile.getFilePath());
        assertThat(fileSystem.newInputFile(dataFileLocation).exists())
                .describedAs("The data file should exist")
                .isTrue();

        assertQuerySucceeds(format("DROP TABLE %s", tableName));
        assertThat(metastore.getTable("tpch", tableName).isPresent())
                .describedAs("Table should be dropped")
                .isFalse();
        assertThat(fileSystem.newInputFile(dataFileLocation).exists())
                .describedAs("The data file should have been removed")
                .isFalse();
        assertThat(fileSystem.newInputFile(tableLocation).exists())
                .describedAs("The directory corresponding to the dropped Iceberg table should not be removed because it may be shared with other tables")
                .isFalse();
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
        assertThat(renamedTableLocation)
                .describedAs("Location should not be changed")
                .isEqualTo(tableInitialLocation);

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
        assertThat(renamedTableLocation)
                .describedAs("Location should not be changed")
                .isEqualTo(tableInitialLocation);

        assertQuerySucceeds(format("CREATE TABLE %s as select 1 as val", tableName));
        Optional<Table> recreatedTableWithInitialName = metastore.getTable("tpch", tableName);
        assertThat(recreatedTableWithInitialName).as("Table should exist").isPresent();
        String recreatedTableLocation = recreatedTableWithInitialName.get().getStorage().getLocation();
        assertThat(tableInitialLocation)
                .describedAs("Location should be different")
                .isNotEqualTo(recreatedTableLocation);
    }
}
