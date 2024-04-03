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
package io.trino.plugin.iceberg.catalog.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testng.services.ManageTestResources;
import io.trino.tpch.TpchTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.lang.String.format;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Redundant over TestIcebergOrcConnectorTest, but exists to exercise BaseConnectorSmokeTest
// Some features like materialized views may be supported by Iceberg only.

public class TestIcebergHadoopConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    @ManageTestResources.Suppress(because = "Not a TestNG test class")
    private TrinoHadoopCatalog hadoopCatalog;
    private File warehouseLocation;

    public TestIcebergHadoopConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory("test_iceberg_hadoop_catalog_smoke_test").toFile();
        hadoopCatalog = (TrinoHadoopCatalog) buildIcebergCatalog("tpch", ImmutableMap.<String, String>builder()
                .put(CATALOG_IMPL, TrinoHadoopCatalog.class.getName())
                    .put(PROPERTY_PREFIX + "user", USER)
                .put(PROPERTY_PREFIX + "password", PASSWORD)
                .put(WAREHOUSE_LOCATION, warehouseLocation.getAbsolutePath())
                .buildOrThrow(),
                new Configuration(false));
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        Map.of("iceberg.file-format", format.name(),
                                "iceberg.catalog.type", "hadoop",
                                "hive.azure.auth-type", "oauth",
                                "hive.azure.oauth.endpoint", "endpoint",
                                "hive.azure.oauth.client-id", "clientId",
                                "hive.azure.oauth.secret", "secret",
                                "iceberg.catalog.warehouse", warehouseLocation.getAbsolutePath()))
                .setInitialTables(ImmutableList.<TpchTable<?>>builder().addAll(REQUIRED_TPCH_TABLES).add(LINE_ITEM).build()).build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_RENAME_TABLE:
                return false;
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
            case SUPPORTS_DELETE:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable).hasStackTraceContaining("Cannot rename Hadoop tables");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas).hasStackTraceContaining("Cannot rename Hadoop tables");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView).hasStackTraceContaining("createMaterializedView is not supported by iceberg");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).isEqualTo("" + "CREATE TABLE iceberg.tpch.region (\n" + "   regionkey bigint,\n" + "   name varchar,\n" + "   comment varchar\n" + ")\n" + "WITH (\n" + "   format = 'ORC'\n" + ")");
    }

    @Override
    protected void deleteDirectory(String location)
    {
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return false;
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(getSession().getSchema().orElse("tpch"), tableName);
        BaseTable table = (BaseTable) hadoopCatalog.loadTable(getSession().toConnectorSession(), schemaTableName);
        return table.operations().current().metadataFileLocation();
    }

    @Override
    protected String schemaPath()
    {
        return format("%s/%s", warehouseLocation, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return false;
    }
}
