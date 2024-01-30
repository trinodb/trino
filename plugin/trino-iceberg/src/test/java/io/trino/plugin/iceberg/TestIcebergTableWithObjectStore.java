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
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Table;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.iceberg.DataFileRecord.toDataFileRecord;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergTableWithObjectStore
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystem fileSystem;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(Map.of(
                        "iceberg.object-store.enabled", "true",
                        "iceberg.data-location", "local:///table-location/xyz"))
                .build();

        metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        return queryRunner;
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
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
        assertThat(dataFile.getFilePath().startsWith("local:///table-location/xyz"))
                .describedAs("The data file's path should start with the configured location")
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
    public void testCreateAndDropWithDifferentDataLocation()
            throws IOException
    {
        String tableName = "test_create_and_drop_with_different_location";
        assertQuerySucceeds(format("CREATE TABLE %s WITH (data_location = 'local:///table-location-2/abc') as select 1 as val", tableName));
        Table table = metastore.getTable("tpch", tableName).orElseThrow();
        assertThat(table.getTableType()).isEqualTo(EXTERNAL_TABLE.name());

        Location tableLocation = Location.of(table.getStorage().getLocation());
        assertThat(fileSystem.newInputFile(tableLocation).exists())
                .describedAs("The directory corresponding to the table storage location should exist")
                .isTrue();

        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_create_and_drop_with_different_location$files\"");
        assertThat(materializedResult.getRowCount()).isEqualTo(1);
        DataFileRecord dataFile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));
        Location dataFileLocation = Location.of(dataFile.getFilePath());
        assertThat(fileSystem.newInputFile(dataFileLocation).exists())
                .describedAs("The data file should exist")
                .isTrue();
        assertThat(dataFile.getFilePath().startsWith("local:///table-location-2/abc"))
                .describedAs("The data file's path should start with the configured location")
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
}
