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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static org.apache.iceberg.FileFormat.ORC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

// Redundant over TestIcebergOrcConnectorTest, but exists to exercise BaseConnectorSmokeTest
// Some features like materialized views may be supported by Iceberg only.
@TestInstance(PER_CLASS)
public class TestIcebergConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private HiveMetastore metastore;

    public TestIcebergConnectorSmokeTest()
    {
        super(ORC);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.file-format", format.name(),
                        "iceberg.register-table-procedure.enabled", "true",
                        "iceberg.writer-sort-buffer-size", "1MB"))
                .build();
        metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        return queryRunner;
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        metastore.dropTable(getSession().getSchema().orElseThrow(), tableName, false);
        assertThat(metastore.getTable(getSession().getSchema().orElseThrow(), tableName)).as("Table in metastore should be dropped").isEmpty();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        return metastore
                .getTable(getSession().getSchema().orElseThrow(), tableName).orElseThrow()
                .getParameters().get("metadata_location");
    }

    @Override
    protected String schemaPath()
    {
        return "local:///%s".formatted(getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            return fileSystem.newInputFile(Location.of(location)).exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }
}
