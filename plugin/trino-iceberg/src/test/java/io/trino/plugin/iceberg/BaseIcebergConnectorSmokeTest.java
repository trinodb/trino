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
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertNotNull;

public abstract class BaseIcebergConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected final FileFormat format;

    protected abstract String getMetadataDirectory(String tableName);

    public BaseIcebergConnectorSmokeTest(FileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return true;

            case SUPPORTS_DELETE:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        // Deletes are covered AbstractTestIcebergConnectorTest
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("" +
                        "CREATE TABLE iceberg." + schemaName + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = '" + format.name() + "',\n" +
                        format("   location = '.*/" + schemaName + "/region'\n") +
                        "\\)");
    }

    @Test
    public void testHiddenPathColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "hidden_file_path", "(a int, b VARCHAR)", ImmutableList.of("(1, 'a')"))) {
            String filePath = (String) computeScalar(format("SELECT file_path FROM \"%s$files\"", table.getName()));

            assertQuery("SELECT DISTINCT \"$path\" FROM " + table.getName(), "VALUES " + "'" + filePath + "'");

            // Check whether the "$path" hidden column is correctly evaluated in the filter expression
            assertQuery(format("SELECT a FROM %s WHERE \"$path\" = '%s'", table.getName(), filePath), "VALUES 1");
        }
    }

    @Test
    public void testListInformationSchemaMetadataNotFound()
            throws Exception
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_metadata_not_found", "(col integer)", ImmutableList.of())) {
            HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(getSession().toConnectorSession());
            org.apache.hadoop.fs.Path metadataDir = new org.apache.hadoop.fs.Path(getMetadataDirectory(table.getName()));
            FileSystem fileSystem = HDFS_ENVIRONMENT.getFileSystem(context, metadataDir);
            org.apache.hadoop.fs.Path metadataFile = getMetadataJsonFile(fileSystem, metadataDir);
            org.apache.hadoop.fs.Path renamedMetadataFile = new org.apache.hadoop.fs.Path(metadataFile + ".renamed");

            fileSystem.rename(metadataFile, renamedMetadataFile);

            assertQueryFails("SELECT * FROM " + table.getName(), "Failed to (read file|open input stream for file): .*");
            assertQuerySucceeds("" +
                    "SELECT table_name FROM information_schema.tables " +
                    "WHERE table_catalog = 'iceberg' AND table_schema = 'tpch' AND table_name = '" + table.getName() + "'");

            // Rename to the original path so that we can clean up in Glue tests
            fileSystem.rename(renamedMetadataFile, metadataFile);
        }
    }

    private org.apache.hadoop.fs.Path getMetadataJsonFile(FileSystem fileSystem, org.apache.hadoop.fs.Path metadataDir)
            throws IOException
    {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(metadataDir, false);
        Path metadataFile = null;
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            if (file.getPath().toString().endsWith(".metadata.json")) {
                metadataFile = file.getPath();
                break;
            }
        }
        assertNotNull(metadataFile);
        return new org.apache.hadoop.fs.Path(metadataFile.toString());
    }
}
