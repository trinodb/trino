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
import io.trino.plugin.hive.HivePlugin;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Integration tests for view redirection between Iceberg and Hive catalogs.
 * When a view exists in the Hive metastore as a Hive/Presto view and is queried
 * through the Iceberg catalog, it should be redirected to the Hive catalog.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestIcebergViewRedirection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        File dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();
        verify(dataDirectory.mkdirs());

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog("iceberg", "iceberg", ImmutableMap.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "hive.metastore.catalog.dir", dataDirectory.getPath(),
                "iceberg.hive-catalog-name", "hive",
                "fs.hadoop.enabled", "true"));
        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of(
                "hive.security", "sql-standard",
                "hive.metastore", "file",
                "hive.metastore.catalog.dir", dataDirectory.getPath(),
                "fs.hadoop.enabled", "true"));

        return queryRunner;
    }

    @BeforeAll
    public void setUp()
    {
        assertQuerySucceeds("CREATE SCHEMA hive.test_schema");
        assertQuerySucceeds("CREATE TABLE hive.test_schema.base_table AS SELECT 1 AS col1, 'hello' AS col2");
        assertQuerySucceeds("CREATE VIEW hive.test_schema.hive_view AS SELECT * FROM hive.test_schema.base_table");
    }

    @AfterAll
    public void tearDown()
    {
        assertQuerySucceeds("DROP VIEW IF EXISTS hive.test_schema.hive_view");
        assertQuerySucceeds("DROP VIEW IF EXISTS hive.test_schema.redirected_view");
        assertQuerySucceeds("DROP VIEW IF EXISTS hive.test_schema.iceberg_own_view");
        assertQuerySucceeds("DROP VIEW IF EXISTS hive.test_schema.view_to_rename");
        assertQuerySucceeds("DROP VIEW IF EXISTS hive.test_schema.view_renamed");
        assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS iceberg.test_schema.test_mv");
        assertQuerySucceeds("DROP TABLE IF EXISTS hive.test_schema.base_table");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive.test_schema");
    }

    @Test
    public void testSelectFromRedirectedView()
    {
        // Query a Hive view through the Iceberg catalog — should redirect
        assertQuery(
                "SELECT * FROM iceberg.test_schema.hive_view",
                "SELECT 1, 'hello'");
    }

    @Test
    public void testShowCreateViewWithRedirection()
    {
        String showCreate = (String) computeScalar("SHOW CREATE VIEW iceberg.test_schema.hive_view");
        assertThat(showCreate).contains("SELECT");
    }

    @Test
    public void testDropViewWithRedirection()
    {
        // Create a view in hive, then drop it via iceberg catalog
        assertQuerySucceeds("CREATE VIEW hive.test_schema.view_to_drop AS SELECT 1 AS x");
        assertQuerySucceeds("DROP VIEW iceberg.test_schema.view_to_drop");
        assertQueryFails("SELECT * FROM hive.test_schema.view_to_drop", ".*does not exist.*");
    }

    @Test
    public void testCreateViewWithRedirection()
    {
        // CREATE VIEW through iceberg catalog should be redirected to hive catalog
        assertQuerySucceeds("CREATE VIEW iceberg.test_schema.redirected_view AS SELECT 42 AS val");
        assertQuery("SELECT * FROM hive.test_schema.redirected_view", "VALUES 42");
        assertQuerySucceeds("DROP VIEW hive.test_schema.redirected_view");
    }

    @Test
    public void testViewRedirectionWithNonExistentView()
    {
        assertQueryFails(
                "SELECT * FROM iceberg.test_schema.nonexistent_view",
                ".*does not exist.*");
    }

    @Test
    public void testIcebergCreatedViewStillWorksViaIceberg()
    {
        // A view created via the Iceberg catalog should still be queryable via Iceberg
        // (even though it gets redirected to hive, the result is the same since they share a metastore)
        assertQuerySucceeds("CREATE VIEW iceberg.test_schema.iceberg_own_view AS SELECT col1 FROM hive.test_schema.base_table");
        assertQuery("SELECT * FROM iceberg.test_schema.iceberg_own_view", "VALUES 1");
        assertQuerySucceeds("DROP VIEW iceberg.test_schema.iceberg_own_view");
    }

    @Test
    public void testMaterializedViewNotAffectedByViewRedirection()
    {
        // Materialized views should work normally and not be confused with view redirection
        assertQuerySucceeds("CREATE MATERIALIZED VIEW iceberg.test_schema.test_mv AS SELECT * FROM hive.test_schema.base_table");
        assertQuery("SELECT * FROM iceberg.test_schema.test_mv", "SELECT 1, 'hello'");
        assertQuerySucceeds("DROP MATERIALIZED VIEW iceberg.test_schema.test_mv");
    }

    @Test
    public void testRenameViewWithRedirection()
    {
        // Create a view in hive, rename it via iceberg catalog
        assertQuerySucceeds("CREATE VIEW hive.test_schema.view_to_rename AS SELECT 1 AS x");
        assertQuerySucceeds("ALTER VIEW iceberg.test_schema.view_to_rename RENAME TO iceberg.test_schema.view_renamed");
        assertQuery("SELECT * FROM hive.test_schema.view_renamed", "VALUES 1");
        assertQueryFails("SELECT * FROM hive.test_schema.view_to_rename", ".*does not exist.*");
        assertQuerySucceeds("DROP VIEW hive.test_schema.view_renamed");
    }
}
