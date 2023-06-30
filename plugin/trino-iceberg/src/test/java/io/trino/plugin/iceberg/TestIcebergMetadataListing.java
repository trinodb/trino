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
import io.trino.Session;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.iceberg.catalog.file.TestingIcebergFileMetastoreCatalogModule;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergMetadataListing
        extends AbstractTestQueryFramework
{
    private FileHiveMetastore metastore;
    private SchemaTableName storageTable;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();

        metastore = createTestingFileHiveMetastore(baseDir);

        queryRunner.installPlugin(new TestingIcebergPlugin(Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore)), Optional.empty(), EMPTY_MODULE));
        queryRunner.createCatalog("iceberg", "iceberg");
        queryRunner.installPlugin(new TestingHivePlugin(metastore));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of("hive.security", "sql-standard"));

        return queryRunner;
    }

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("CREATE SCHEMA hive.test_schema");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.iceberg_table1 (_string VARCHAR, _integer INTEGER)");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.iceberg_table2 (_double DOUBLE) WITH (partitioning = ARRAY['_double'])");
        assertQuerySucceeds("CREATE MATERIALIZED VIEW iceberg.test_schema.iceberg_materialized_view AS " +
                "SELECT * FROM iceberg.test_schema.iceberg_table1");
        storageTable = getStorageTable("iceberg", "test_schema", "iceberg_materialized_view");
        assertQuerySucceeds("CREATE VIEW iceberg.test_schema.iceberg_view AS SELECT * FROM iceberg.test_schema.iceberg_table1");

        assertQuerySucceeds("CREATE TABLE hive.test_schema.hive_table (_double DOUBLE)");
        assertQuerySucceeds("CREATE VIEW hive.test_schema.hive_view AS SELECT * FROM hive.test_schema.hive_table");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive.test_schema.hive_table");
        assertQuerySucceeds("DROP VIEW IF EXISTS hive.test_schema.hive_view");
        assertQuerySucceeds("DROP VIEW IF EXISTS iceberg.test_schema.iceberg_view");
        assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS iceberg.test_schema.iceberg_materialized_view");
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.iceberg_table2");
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.iceberg_table1");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive.test_schema");
    }

    @Test
    public void testTableListing()
    {
        assertThat(metastore.getAllTables("test_schema"))
                .containsExactlyInAnyOrder(
                        "iceberg_table1",
                        "iceberg_table2",
                        "iceberg_materialized_view",
                        storageTable.getTableName(),
                        "iceberg_view",
                        "hive_table",
                        "hive_view");

        assertQuery(
                "SHOW TABLES FROM iceberg.test_schema",
                "VALUES " +
                        "'iceberg_table1', " +
                        "'iceberg_table2', " +
                        "'iceberg_materialized_view', " +
                        "'" + storageTable.getTableName() + "', " +
                        "'iceberg_view', " +
                        "'hive_table', " +
                        "'hive_view'");
    }

    @Test
    public void testTableColumnListing()
    {
        // Verify information_schema.columns does not include columns from non-Iceberg tables
        assertQuery(
                "SELECT table_name, column_name FROM iceberg.information_schema.columns WHERE table_schema = 'test_schema'",
                "VALUES " +
                        "('iceberg_table1', '_string'), " +
                        "('iceberg_table1', '_integer'), " +
                        "('iceberg_table2', '_double'), " +
                        "('iceberg_materialized_view', '_string'), " +
                        "('iceberg_materialized_view', '_integer'), " +
                        "('" + storageTable.getTableName() + "', '_string'), " +
                        "('" + storageTable.getTableName() + "', '_integer'), " +
                        "('iceberg_view', '_string'), " +
                        "('iceberg_view', '_integer'), " +
                        "('hive_view', '_double')");
    }

    @Test
    public void testTableDescribing()
    {
        assertQuery("DESCRIBE iceberg.test_schema.iceberg_table1", "VALUES ('_string', 'varchar', '', ''), ('_integer', 'integer', '', '')");
    }

    @Test
    public void testTableValidation()
    {
        assertQuerySucceeds("SELECT * FROM iceberg.test_schema.iceberg_table1");
        assertQueryFails("SELECT * FROM iceberg.test_schema.hive_table", "Not an Iceberg table: test_schema.hive_table");
    }

    private SchemaTableName getStorageTable(String catalogName, String schemaName, String objectName)
    {
        TransactionManager transactionManager = getQueryRunner().getTransactionManager();
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = getSession().beginTransactionId(transactionId, transactionManager, getQueryRunner().getAccessControl());
        Optional<MaterializedViewDefinition> materializedView = getQueryRunner().getMetadata()
                .getMaterializedView(session, new QualifiedObjectName(catalogName, schemaName, objectName));
        assertThat(materializedView).isPresent();
        return materializedView.get().getStorageTable().get().getSchemaTableName();
    }
}
