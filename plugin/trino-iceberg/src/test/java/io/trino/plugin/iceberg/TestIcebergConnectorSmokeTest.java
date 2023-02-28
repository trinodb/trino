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
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.ORC;
import static org.assertj.core.api.Assertions.assertThat;

// Redundant over TestIcebergOrcConnectorTest, but exists to exercise BaseConnectorSmokeTest
// Some features like materialized views may be supported by Iceberg only.
public class TestIcebergConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private HiveMetastore metastore;
    private File metastoreDir;

    public TestIcebergConnectorSmokeTest()
    {
        super(ORC);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.metastoreDir = Files.createTempDirectory("test_iceberg_table_smoke_test").toFile();
        this.metastoreDir.deleteOnExit();
        this.metastore = createTestingFileHiveMetastore(metastoreDir);
        return IcebergQueryRunner.builder()
                .setInitialTables(ImmutableList.<TpchTable<?>>builder()
                        .addAll(REQUIRED_TPCH_TABLES)
                        .add(LINE_ITEM)
                        .build())
                .setMetastoreDirectory(metastoreDir)
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.register-table-procedure.enabled", "true",
                        "iceberg.writer-sort-buffer-size", "1MB"))
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
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
        return format("%s/%s", metastoreDir, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return Files.exists(Path.of(location));
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            deleteRecursively(Path.of(location), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkOrcFileSorting(path, sortColumnName);
    }
}
