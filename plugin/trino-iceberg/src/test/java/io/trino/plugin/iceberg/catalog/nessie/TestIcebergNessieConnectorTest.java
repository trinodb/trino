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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.BaseIcebergConnectorTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.NessieContainer;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergNessieConnectorTest
        extends BaseIcebergConnectorTest
{
    private static NessieContainer nessieContainer;
    private static Path tempDir;

    public TestIcebergNessieConnectorTest()
    {
        super(PARQUET);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        nessieContainer = closeAfterClass(NessieContainer.builder().build());
        nessieContainer.start();
        tempDir = Files.createTempDirectory("test_trino_nessie");
        super.init();
    }

    @AfterClass
    public void teardown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(tempDir))
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.file-format", PARQUET.name(),
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie.uri", nessieContainer.getRestApiUri(),
                                "iceberg.nessie.default-warehouse-dir", tempDir.resolve("iceberg_data").toString()))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.<TpchTable<?>>builder().addAll(REQUIRED_TPCH_TABLES).add(LINE_ITEM).build())
                                .build())
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_RENAME_SCHEMA:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return true;
    }

    @Override
    protected Session withSmallRowGroups(Session session)
    {
        return session;
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return false;
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg.tpch");
    }

    @Test
    @Override
    public void testCreateTableSchemaNotFound()
    {
        // creating a table without creating a namespace/schema first works in nessie
        String schemaName = "test_schema_" + randomTableSuffix();
        String tableName = "test_create_no_schema_" + randomTableSuffix();
        try {
            // schema shouldn't exist
            assertThatThrownBy(() -> computeActual(format("SHOW CREATE SCHEMA %s", schemaName)).getOnlyValue().toString())
                    .hasMessageContaining(format("Schema 'iceberg.%s' does not exist", schemaName));

            assertQuerySucceeds(format("CREATE TABLE %s.%s (a bigint)", schemaName, tableName));

            // schema should exist now
            assertThat(computeActual(format("SHOW CREATE SCHEMA %s", schemaName)).getOnlyValue().toString())
                    .matches(format("CREATE SCHEMA iceberg.%s", schemaName));
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
        }
    }

    @Test
    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        // creating a table without creating a namespace/schema first works in nessie
        String schemaName = "test_schema_" + randomTableSuffix();
        String tableName = "test_ctas_no_schema_" + randomTableSuffix();
        try {
            // schema shouldn't exist
            assertThatThrownBy(() -> computeActual(format("SHOW CREATE SCHEMA %s", schemaName)).getOnlyValue().toString())
                    .hasMessageContaining(format("Schema 'iceberg.%s' does not exist", schemaName));

            assertQuerySucceeds(format("CREATE TABLE %s.%s AS SELECT name FROM nation", schemaName, tableName));

            // schema should exist now
            assertThat(computeActual(format("SHOW CREATE SCHEMA %s", schemaName)).getOnlyValue().toString())
                    .matches(format("CREATE SCHEMA iceberg.%s", schemaName));
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
        }
    }
}
