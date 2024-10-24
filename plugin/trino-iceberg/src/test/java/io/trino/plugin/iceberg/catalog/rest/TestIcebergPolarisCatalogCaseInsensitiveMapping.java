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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergSchemaProperties;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ViewBuilder;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.SCOPE;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergPolarisCatalogCaseInsensitiveMapping
        extends AbstractTestQueryFramework
{
    private static final SessionContext SESSION_CONTEXT = new SessionContext("dummy", null, null, ImmutableMap.of(), "user");
    private RESTSessionCatalog icebergCatalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File warehouseLocation = Files.newTemporaryFolder();
        TestingPolarisCatalog polarisCatalog = closeAfterClass(new TestingPolarisCatalog(warehouseLocation.getPath()));

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, polarisCatalog.restUri() + "/api/catalog")
                .put(CatalogProperties.WAREHOUSE_LOCATION, TestingPolarisCatalog.WAREHOUSE)
                .put(CREDENTIAL, polarisCatalog.oauth2Credentials())
                .put(SCOPE, "PRINCIPAL_ROLE:ALL")
                .buildOrThrow();

        RESTSessionCatalog icebergCatalogInstance = new RESTSessionCatalog(
                config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build(), null);
        icebergCatalogInstance.initialize("test_catalog", properties);

        icebergCatalog = icebergCatalogInstance;
        closeAfterClass(icebergCatalog);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", polarisCatalog.restUri() + "/api/catalog")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", TestingPolarisCatalog.WAREHOUSE)
                .addIcebergProperty("iceberg.rest-catalog.security", "OAUTH2")
                .addIcebergProperty("iceberg.rest-catalog.oauth2.credential", polarisCatalog.oauth2Credentials())
                .addIcebergProperty("iceberg.rest-catalog.oauth2.scope", "PRINCIPAL_ROLE:ALL")
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("iceberg.rest-catalog.case-insensitive-name-matching", "true")
                .build();
    }

    @Test
    void testCaseInSensitiveMatching()
    {
        icebergCatalog.createNamespace(SESSION_CONTEXT, Namespace.of("LeVeL1"));
        icebergCatalog.createNamespace(SESSION_CONTEXT, Namespace.of("LeVeL1", "LeVeL2"));
        icebergCatalog.createNamespace(SESSION_CONTEXT, Namespace.of("LeVeL1", "LeVeL2", "LeVeL3"));

        List<Namespace> namespaces = icebergCatalog.listNamespaces(SESSION_CONTEXT, Namespace.of("LeVeL1", "LeVeL2"));
        assertThat(getOnlyElement(namespaces)).isEqualTo(Namespace.of("LeVeL1", "LeVeL2", "LeVeL3"));
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).isEqualTo(Set.of("information_schema", "tpch", "level1", "level1.level2", "level1.level2.level3"));
        assertThat(computeActual("SHOW SCHEMAS LIKE 'level%'").getOnlyColumnAsSet()).isEqualTo(Set.of("level1", "level1.level2", "level1.level2.level3"));

        Map<String, String> level3NamespaceMetadata = icebergCatalog.loadNamespaceMetadata(SESSION_CONTEXT, Namespace.of("LeVeL1", "LeVeL2", "LeVeL3"));
        String level3NamespaceLocation = level3NamespaceMetadata.get(IcebergSchemaProperties.LOCATION_PROPERTY);

        // Create and query a mixed case letter table from Trino
        String tableName1 = "MiXed_CaSe_TaBlE1_" + randomNameSuffix();
        String lowercaseTableName1 = tableName1.toLowerCase(Locale.ENGLISH);
        String table1Location = level3NamespaceLocation + "/" + lowercaseTableName1;
        assertUpdate("CREATE TABLE iceberg.\"level1.level2.level3\"." + tableName1 + " WITH (location = '" + table1Location + "') AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertQuery("SELECT * FROM iceberg.\"level1.level2.level3\"." + tableName1, "VALUES (42, -38.5)");

        // Create a mixed case letter table directly using rest catalog and query from Trino
        String tableName2 = "mIxEd_cAsE_tAbLe2_" + randomNameSuffix();
        String lowercaseTableName2 = tableName2.toLowerCase(Locale.ENGLISH);
        String table2Location = level3NamespaceLocation + "/" + lowercaseTableName2;
        createDir(table2Location);
        createDir(table2Location + "/data");
        createDir(table2Location + "/metadata");
        icebergCatalog
                .buildTable(SESSION_CONTEXT, TableIdentifier.of(Namespace.of("LeVeL1", "LeVeL2", "LeVeL3"), tableName2), new Schema(required(1, "x", Types.LongType.get())))
                .withLocation(table2Location)
                .createTransaction()
                .commitTransaction();
        assertUpdate("INSERT INTO iceberg.\"level1.level2.level3\"." + tableName2 + " VALUES (78)", 1);
        assertQuery("SELECT * FROM iceberg.\"level1.level2.level3\"." + tableName2, "VALUES (78)");

        // Test register/unregister table. Re-register for further testing.
        assertThat(icebergCatalog.dropTable(SESSION_CONTEXT, TableIdentifier.of(Namespace.of("LeVeL1", "LeVeL2", "LeVeL3"), lowercaseTableName1))).isTrue();
        assertQueryFails("SELECT * FROM iceberg.\"level1.level2.level3\"." + tableName1, ".*'iceberg.\"level1.level2.level3\".%s' does not exist".formatted(lowercaseTableName1));
        assertUpdate("CALL system.register_table ('level1.level2.level3', '" + tableName1 + "', '" + table1Location + "')");
        assertQuery("SELECT * FROM iceberg.\"level1.level2.level3\"." + tableName1, "VALUES (42, -38.5)");
        assertUpdate("CALL system.unregister_table ('level1.level2.level3', '" + tableName1 + "')");
        assertQueryFails("SELECT * FROM iceberg.\"level1.level2.level3\"." + tableName1, ".*'iceberg.\"level1.level2.level3\".%s' does not exist".formatted(lowercaseTableName1));
        assertUpdate("CALL system.register_table ('level1.level2.level3', '" + tableName1 + "', '" + table1Location + "')");

        // Create and query a mixed case letter view from Trino
        String viewName1 = "MiXed_CaSe_vIeW_1" + randomNameSuffix();
        String lowercaseViewName1 = viewName1.toLowerCase(Locale.ENGLISH);
        assertUpdate("CREATE VIEW iceberg.\"level1.level2.level3\"." + viewName1 + " AS SELECT BIGINT '25' a, DOUBLE '99.4' b");
        assertQuery("SELECT * FROM iceberg.\"level1.level2.level3\"." + viewName1, "VALUES (25, 99.4)");

        // Create a mixed case letter view directly using rest catalog and query from Trino
        String viewName2 = "mIxEd_cAsE_ViEw_2" + randomNameSuffix();
        String lowercaseViewName2 = viewName2.toLowerCase(Locale.ENGLISH);
        String view2Location = level3NamespaceLocation + "/" + lowercaseViewName2;
        createDir(view2Location);
        createDir(view2Location + "/data");
        createDir(view2Location + "/metadata");
        ViewBuilder viewBuilder = icebergCatalog.buildView(SESSION_CONTEXT, TableIdentifier.of(Namespace.of("LeVeL1", "LeVeL2", "LeVeL3"), viewName2));
        viewBuilder
                .withQuery("trino", "SELECT BIGINT '34' y")
                .withSchema(new Schema(required(1, "y", Types.LongType.get())))
                .withDefaultNamespace(Namespace.of("LeVeL1", "LeVeL2", "LeVeL3"))
                .withLocation(view2Location)
                .createOrReplace();
        assertQuery("SELECT * FROM iceberg.\"level1.level2.level3\"." + viewName2, "VALUES (34)");

        // Query information_schema and list objects
        assertThat(computeActual("SHOW TABLES IN iceberg.\"level1.level2.level3\"").getOnlyColumnAsSet()).isEqualTo(Set.of(lowercaseTableName1, lowercaseTableName2, lowercaseViewName1, lowercaseViewName2));
        assertThat(computeActual("SHOW TABLES IN iceberg.\"level1.level2.level3\" LIKE 'mixed_case%'").getOnlyColumnAsSet()).isEqualTo(Set.of(lowercaseTableName1, lowercaseTableName2, lowercaseViewName1, lowercaseViewName2));
        assertQuery("SELECT * FROM iceberg.information_schema.schemata",
                """
                        VALUES
                        ('iceberg', 'information_schema'),
                        ('iceberg', 'level1'),
                        ('iceberg', 'level1.level2'),
                        ('iceberg', 'level1.level2.level3'),
                        ('iceberg', 'tpch')
                        """);
        assertQuery("SELECT * FROM iceberg.information_schema.tables WHERE table_schema != 'information_schema'",
                ("VALUES " +
                        "('iceberg', 'level1.level2.level3', '%s', 'BASE TABLE'),").formatted(lowercaseTableName1) +
                        "('iceberg', 'level1.level2.level3', '%s', 'BASE TABLE'),".formatted(lowercaseTableName2) +
                        "('iceberg', 'level1.level2.level3', '%s', 'VIEW'),".formatted(lowercaseViewName1) +
                        "('iceberg', 'level1.level2.level3', '%s', 'VIEW')".formatted(lowercaseViewName2));

        // Drop created tables/views
        assertUpdate("DROP TABLE iceberg.\"level1.level2.level3\"." + tableName1);
        assertUpdate("DROP TABLE iceberg.\"level1.level2.level3\"." + tableName2);
        assertUpdate("DROP VIEW iceberg.\"level1.level2.level3\"." + viewName1);
        assertUpdate("DROP VIEW iceberg.\"level1.level2.level3\"." + viewName2);

        // Query dropped tables/views
        assertQueryFails("SELECT * FROM iceberg.\"level1.level2.level3\"." + tableName1, ".*'iceberg.\"level1.level2.level3\".%s' does not exist".formatted(lowercaseTableName1));
        assertQueryFails("SELECT * FROM iceberg.\"level1.level2.level3\"." + tableName2, ".*'iceberg.\"level1.level2.level3\".%s' does not exist".formatted(lowercaseTableName2));
        assertQueryFails("SELECT * FROM iceberg.\"level1.level2.level3\"." + viewName1, ".*'iceberg.\"level1.level2.level3\".%s' does not exist".formatted(lowercaseViewName1));
        assertQueryFails("SELECT * FROM iceberg.\"level1.level2.level3\"." + viewName2, ".*'iceberg.\"level1.level2.level3\".%s' does not exist".formatted(lowercaseViewName2));
    }

    private static void createDir(String absoluteDirPath)
    {
        Path path = Paths.get(URI.create(absoluteDirPath).getPath());
        try {
            java.nio.file.Files.createDirectories(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot create %s directory".formatted(absoluteDirPath), e);
        }
    }
}
