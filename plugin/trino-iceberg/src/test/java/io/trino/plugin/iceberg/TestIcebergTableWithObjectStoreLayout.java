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
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergTableWithObjectStoreLayout
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystem fileSystem;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.object-store-layout.enabled", "true")
                .build();

        metastore = getHiveMetastore(queryRunner);

        fileSystem = getFileSystemFactory(queryRunner).create(SESSION);

        return queryRunner;
    }

    @Test
    void testCreateTableWithDataLocation()
            throws Exception
    {
        assertQuerySucceeds("CREATE TABLE test_create_table_with_different_location WITH (data_location = 'local:///table-location/abc') AS SELECT 1 AS val");
        Table table = metastore.getTable("tpch", "test_create_table_with_different_location").orElseThrow();
        assertThat(table.getTableType()).isEqualTo(EXTERNAL_TABLE.name());

        Location tableLocation = Location.of(table.getStorage().getLocation());
        assertThat(fileSystem.newInputFile(tableLocation).exists()).isTrue();

        String filePath = (String) computeScalar("SELECT file_path FROM \"test_create_table_with_different_location$files\"");
        Location dataFileLocation = Location.of(filePath);
        assertThat(fileSystem.newInputFile(dataFileLocation).exists()).isTrue();
        assertThat(filePath).matches("local:///table-location/abc/.{6}/tpch/test_create_table_with_different_location-.*/.*\\.parquet");

        assertQuerySucceeds("DROP TABLE test_create_table_with_different_location");
        assertThat(metastore.getTable("tpch", "test_create_table_with_different_location")).isEmpty();
        assertThat(fileSystem.newInputFile(dataFileLocation).exists()).isFalse();
        assertThat(fileSystem.newInputFile(tableLocation).exists()).isFalse();
    }
}
