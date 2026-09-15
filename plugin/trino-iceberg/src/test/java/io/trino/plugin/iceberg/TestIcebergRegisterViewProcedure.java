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
import io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogConfig.SchemaVersion;
import io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergRegisterViewProcedure
        extends AbstractTestQueryFramework
{
    private File warehouseLocation;
    private JdbcCatalog jdbcCatalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory("test_iceberg_register_view").toFile();
        closeAfterClass(() -> deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE));
        TestingIcebergJdbcServer server = closeAfterClass(new TestingIcebergJdbcServer());
        jdbcCatalog = (JdbcCatalog) buildIcebergCatalog(
                "iceberg",
                ImmutableMap.<String, String>builder()
                        .put(CATALOG_IMPL, JdbcCatalog.class.getName())
                        .put(URI, server.getJdbcUrl())
                        .put(PROPERTY_PREFIX + "user", USER)
                        .put(PROPERTY_PREFIX + "password", PASSWORD)
                        .put(PROPERTY_PREFIX + "schema-version", SchemaVersion.V1.toString())
                        .put(WAREHOUSE_LOCATION, warehouseLocation.getAbsolutePath())
                        .buildOrThrow(),
                null);
        closeAfterClass(jdbcCatalog);

        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "jdbc")
                                .put("iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver")
                                .put("iceberg.jdbc-catalog.connection-url", server.getJdbcUrl())
                                .put("iceberg.jdbc-catalog.connection-user", USER)
                                .put("iceberg.jdbc-catalog.connection-password", PASSWORD)
                                .put("iceberg.jdbc-catalog.catalog-name", "iceberg")
                                .put("iceberg.jdbc-catalog.schema-version", SchemaVersion.V1.toString())
                                .put("iceberg.jdbc-catalog.default-warehouse-dir", warehouseLocation.getAbsolutePath())
                                .put("iceberg.register-view-procedure.enabled", "true")
                                .buildOrThrow())
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .build();
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.tpch");
        return queryRunner;
    }

    @AfterAll
    public final void destroy()
    {
        jdbcCatalog = null; // closed by closeAfterClass
    }

    @Test
    public void testRegisterViewWithViewLocation()
    {
        String sourceViewName = "test_register_view_source_" + randomNameSuffix();
        String registeredViewName = "test_register_view_registered_" + randomNameSuffix();

        assertUpdate("CREATE VIEW " + sourceViewName + " AS SELECT nationkey, name FROM tpch.tiny.nation");
        String viewLocation = loadViewMetadata(sourceViewName).location();

        assertUpdate(format("CALL iceberg.system.register_view(CURRENT_SCHEMA, '%s', '%s')", registeredViewName, viewLocation));

        assertThat(query("SELECT nationkey FROM " + registeredViewName + " WHERE name = 'ALGERIA'"))
                .matches("VALUES BIGINT '0'");

        // The source view continues to work; both catalog entries share the on-disk metadata
        assertThat(query("SELECT nationkey FROM " + sourceViewName + " WHERE name = 'ALGERIA'"))
                .matches("VALUES BIGINT '0'");

        // Both catalog entries point at the same metadata file; dropping the source purges those
        // files, so we drop only the source and let the shared cleanup deal with the leftover
        // registered catalog entry.
        assertUpdate("DROP VIEW " + sourceViewName);
    }

    @Test
    public void testRegisterViewWithMetadataFile()
    {
        String sourceViewName = "test_register_view_with_metadata_file_" + randomNameSuffix();
        String registeredViewName = "test_register_view_with_metadata_file_registered_" + randomNameSuffix();

        assertUpdate("CREATE VIEW " + sourceViewName + " AS SELECT nationkey FROM tpch.tiny.nation");
        ViewMetadata viewMetadata = loadViewMetadata(sourceViewName);
        String viewLocation = viewMetadata.location();
        String metadataLocation = viewMetadata.metadataFileLocation();
        String metadataFileName = metadataLocation.substring(metadataLocation.lastIndexOf('/') + 1);

        assertUpdate(format(
                "CALL iceberg.system.register_view(CURRENT_SCHEMA, '%s', '%s', '%s')",
                registeredViewName,
                viewLocation,
                metadataFileName));

        assertThat(query("SELECT nationkey FROM " + registeredViewName + " WHERE nationkey = 0"))
                .matches("VALUES BIGINT '0'");

        assertUpdate("DROP VIEW " + sourceViewName);
    }

    @Test
    public void testRegisterViewShowCreateViewMatches()
    {
        String sourceViewName = "test_register_view_show_" + randomNameSuffix();
        String registeredViewName = "test_register_view_show_registered_" + randomNameSuffix();

        assertUpdate("CREATE VIEW " + sourceViewName + " AS SELECT nationkey FROM tpch.tiny.nation");
        String showCreateSource = (String) computeActual("SHOW CREATE VIEW " + sourceViewName).getOnlyValue();
        String viewLocation = loadViewMetadata(sourceViewName).location();

        assertUpdate(format("CALL iceberg.system.register_view(CURRENT_SCHEMA, '%s', '%s')", registeredViewName, viewLocation));

        String showCreateRegistered = (String) computeActual("SHOW CREATE VIEW " + registeredViewName).getOnlyValue();
        // The SQL body and columns should match; only the view name and location property differ
        assertThat(showCreateRegistered.replace(registeredViewName, sourceViewName))
                .isEqualTo(showCreateSource);

        assertUpdate("DROP VIEW " + sourceViewName);
    }

    @Test
    public void testRegisterViewWithNonExistentSchema()
    {
        assertQueryFails(
                "CALL iceberg.system.register_view('nonexistent_schema', 'view_name', '/tmp/view')",
                "Schema 'nonexistent_schema' does not exist");
    }

    @Test
    public void testRegisterViewWithNonExistentLocation()
    {
        String location = "/tmp/does_not_exist_" + randomNameSuffix();
        assertQueryFails(
                format("CALL iceberg.system.register_view(CURRENT_SCHEMA, 'view_name', '%s')", location),
                ".*No versioned metadata file exists at location.*");
    }

    @Test
    public void testRegisterViewWithInvalidMetadataFile()
    {
        String sourceViewName = "test_register_view_invalid_metadata_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + sourceViewName + " AS SELECT nationkey FROM tpch.tiny.nation");
        String viewLocation = loadViewMetadata(sourceViewName).location();

        assertQueryFails(
                format("CALL iceberg.system.register_view(CURRENT_SCHEMA, 'name', '%s', 'not_a_real_file.metadata.json')", viewLocation),
                ".*Metadata file does not exist.*");

        assertUpdate("DROP VIEW " + sourceViewName);
    }

    @Test
    public void testRegisterViewWithInvalidMetadataFileName()
    {
        assertQueryFails(
                "CALL iceberg.system.register_view(CURRENT_SCHEMA, 'name', '/tmp/view', 'has/slash.metadata.json')",
                ".*has/slash.metadata.json is not a valid metadata file.*");
    }

    @Test
    public void testRegisterViewAccessControl()
    {
        String sourceViewName = "test_register_view_access_control_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + sourceViewName + " AS SELECT nationkey FROM tpch.tiny.nation");
        String viewLocation = loadViewMetadata(sourceViewName).location();

        assertAccessDenied(
                format("CALL iceberg.system.register_view(CURRENT_SCHEMA, 'target', '%s')", viewLocation),
                "Cannot create view iceberg\\.tpch\\.target",
                privilege("target", CREATE_VIEW));

        assertUpdate("DROP VIEW " + sourceViewName);
    }

    private ViewMetadata loadViewMetadata(String viewName)
    {
        String schemaName = getSession().getSchema().orElseThrow();
        View view = jdbcCatalog.loadView(TableIdentifier.of(schemaName, viewName));
        return ((BaseView) view).operations().current();
    }
}
