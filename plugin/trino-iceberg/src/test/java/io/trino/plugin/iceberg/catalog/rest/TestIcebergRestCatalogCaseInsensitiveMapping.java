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

import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ViewBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createDirectories;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergRestCatalogCaseInsensitiveMapping
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "LeVeL1_" + randomNameSuffix();
    private static final String LOWERCASE_SCHEMA = SCHEMA.toLowerCase(ENGLISH);
    private static final Namespace NAMESPACE = Namespace.of(SCHEMA);

    private JdbcCatalog backend;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder(LOWERCASE_SCHEMA)
                .setBaseDataDir(Optional.of(warehouseLocation))
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .addIcebergProperty("iceberg.rest-catalog.case-insensitive-name-matching", "true")
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .build();
    }

    @BeforeAll
    void setup()
    {
        backend.createNamespace(NAMESPACE);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(
                        "information_schema",
                        "tpch",
                        LOWERCASE_SCHEMA,
                        "system");

        assertThat(computeActual("SHOW SCHEMAS LIKE 'level%'").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(
                        LOWERCASE_SCHEMA);

        assertQuery("SELECT * FROM information_schema.schemata",
                        """
                        VALUES
                        ('iceberg', 'information_schema'),
                        ('iceberg', 'system'),
                        ('iceberg', '%s'),
                        ('iceberg', 'tpch')
                        """.formatted(LOWERCASE_SCHEMA));
    }

    @Test
    void testCaseInsensitiveMatchingForTable()
    {
        Map<String, String> namespaceMetadata = backend.loadNamespaceMetadata(NAMESPACE);
        String namespaceLocation = namespaceMetadata.get(LOCATION_PROPERTY);
        createDir(namespaceLocation);

        // Create and query a mixed case letter table from Trino
        String tableName1 = "MiXed_CaSe_TaBlE1_" + randomNameSuffix();
        String lowercaseTableName1 = tableName1.toLowerCase(ENGLISH);
        String table1Location = namespaceLocation + "/" + lowercaseTableName1;
        assertUpdate("CREATE TABLE " + tableName1 + " WITH (location = '" + table1Location + "') AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertQuery("SELECT * FROM " + tableName1, "VALUES (42, -38.5)");

        // Create a mixed case letter table directly using rest catalog and query from Trino
        String tableName2 = "mIxEd_cAsE_tAbLe2_" + randomNameSuffix();
        String lowercaseTableName2 = tableName2.toLowerCase(ENGLISH);
        String table2Location = namespaceLocation + "/" + lowercaseTableName2;
        createDir(table2Location);
        createDir(table2Location + "/data");
        createDir(table2Location + "/metadata");
        backend
                .buildTable(TableIdentifier.of(NAMESPACE, tableName2), new Schema(required(1, "x", Types.LongType.get())))
                .withLocation(table2Location)
                .createTransaction()
                .commitTransaction();
        assertUpdate("INSERT INTO " + tableName2 + " VALUES (78)", 1);
        assertQuery("SELECT * FROM " + tableName2, "VALUES (78)");

        // Test register/unregister table. Re-register for further testing.
        assertThat(backend.dropTable(TableIdentifier.of(NAMESPACE, lowercaseTableName1), false)).isTrue();
        assertQueryFails("SELECT * FROM " + tableName1, ".*'iceberg.%s.%s' does not exist".formatted(LOWERCASE_SCHEMA, lowercaseTableName1));
        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName1 + "', '" + table1Location + "')");
        assertQuery("SELECT * FROM " + tableName1, "VALUES (42, -38.5)");
        assertUpdate("CALL system.unregister_table (CURRENT_SCHEMA, '" + tableName1 + "')");
        assertQueryFails("SELECT * FROM " + tableName1, ".*'iceberg.%s.%s' does not exist".formatted(LOWERCASE_SCHEMA, lowercaseTableName1));
        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName1 + "', '" + table1Location + "')");

        // Query information_schema and list objects
        assertThat(computeActual("SHOW TABLES IN " + SCHEMA).getOnlyColumnAsSet()).contains(lowercaseTableName1, lowercaseTableName2);
        assertThat(computeActual("SHOW TABLES IN " + SCHEMA + " LIKE 'mixed_case_table%'").getOnlyColumnAsSet()).isEqualTo(Set.of(lowercaseTableName1, lowercaseTableName2));
        assertQuery("SELECT * FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'system') AND table_type = 'BASE TABLE'",
                        """
                        VALUES
                        ('iceberg', '%1$s', '%2$s', 'BASE TABLE'),
                        ('iceberg', '%1$s', '%3$s', 'BASE TABLE')
                        """.formatted(LOWERCASE_SCHEMA, lowercaseTableName1, lowercaseTableName2));

        // Add table comment
        assertUpdate("COMMENT ON TABLE " +  tableName1 + " IS 'test comment' ");
        assertThat(getTableComment(lowercaseTableName1)).isEqualTo("test comment");

        // Add table column comment
        assertUpdate("COMMENT ON COLUMN " + tableName1 + ".a IS 'test column comment'");
        assertThat(getColumnComment(lowercaseTableName1, "a")).isEqualTo("test column comment");

        // Rename table
        String renamedTableName1 = tableName1 + "_renamed";
        assertUpdate("ALTER TABLE " + lowercaseTableName1 + " RENAME TO " + renamedTableName1);
        assertQueryFails("SELECT * FROM " + tableName1, ".*'iceberg.%s.%s' does not exist".formatted(LOWERCASE_SCHEMA, lowercaseTableName1));
        assertQuery("SELECT * FROM " + renamedTableName1, "VALUES (42, -38.5)");

        // Drop tables
        assertUpdate("DROP TABLE " + renamedTableName1);
        assertUpdate("DROP TABLE " + tableName2);

        // Query dropped tablesd
        assertQueryFails("SELECT * FROM " + renamedTableName1, ".*'iceberg.%s.%s' does not exist".formatted(LOWERCASE_SCHEMA, renamedTableName1.toLowerCase(ENGLISH)));
        assertQueryFails("SELECT * FROM " + tableName2, ".*'iceberg.%s.%s' does not exist".formatted(LOWERCASE_SCHEMA, lowercaseTableName2));
    }

    @Test
    void testCaseInsensitiveMatchingForView()
    {
        Map<String, String> namespaceMetadata = backend.loadNamespaceMetadata(NAMESPACE);
        String namespaceLocation = namespaceMetadata.get(LOCATION_PROPERTY);
        createDir(namespaceLocation);

        // Create and query a mixed case letter view from Trino
        String viewName1 = "MiXed_CaSe_vIeW1_" + randomNameSuffix();
        String lowercaseViewName1 = viewName1.toLowerCase(ENGLISH);
        assertUpdate("CREATE VIEW " + viewName1 + " AS SELECT BIGINT '25' a, DOUBLE '99.4' b");
        assertQuery("SELECT * FROM " + viewName1, "VALUES (25, 99.4)");

        // Create a mixed case letter view directly using rest catalog and query from Trino
        String viewName2 = "mIxEd_cAsE_ViEw2_" + randomNameSuffix();
        String lowercaseViewName2 = viewName2.toLowerCase(ENGLISH);
        String view2Location = namespaceLocation + "/" + lowercaseViewName2;
        createDir(view2Location);
        createDir(view2Location + "/data");
        createDir(view2Location + "/metadata");
        ViewBuilder viewBuilder = backend.buildView(TableIdentifier.of(NAMESPACE, viewName2));
        viewBuilder
                .withQuery("trino", "SELECT BIGINT '34' y")
                .withSchema(new Schema(required(1, "y", Types.LongType.get())))
                .withDefaultNamespace(NAMESPACE)
                .withLocation(view2Location)
                .createOrReplace();
        assertQuery("SELECT * FROM " + viewName2, "VALUES (34)");

        // Query information_schema and list objects
        assertThat(computeActual("SHOW TABLES IN " + SCHEMA).getOnlyColumnAsSet()).contains(lowercaseViewName1, lowercaseViewName2);
        assertThat(computeActual("SHOW TABLES IN " + SCHEMA + " LIKE 'mixed_case_view%'").getOnlyColumnAsSet()).contains(lowercaseViewName1, lowercaseViewName2);
        assertQuery("SELECT * FROM information_schema.tables WHERE table_schema != 'information_schema' AND table_type = 'VIEW'",
                        """
                         VALUES
                         ('iceberg', '%1$s', '%2$s', 'VIEW'),
                         ('iceberg', '%1$s', '%3$s', 'VIEW')
                         """.formatted(LOWERCASE_SCHEMA, lowercaseViewName1, lowercaseViewName2));

        // Add view comment
        assertUpdate("COMMENT ON VIEW " + viewName1 + " IS 'test comment' ");
        assertThat(getTableComment(lowercaseViewName1)).isEqualTo("test comment");

        // Add view column comment
        assertUpdate("COMMENT ON COLUMN " + viewName1 + ".a IS 'test column comment'");
        assertThat(getColumnComment(lowercaseViewName1, "a")).isEqualTo("test column comment");

        // Rename view
        String renamedViewName1 = viewName1 + "_renamed";
        assertUpdate("ALTER VIEW " + lowercaseViewName1 + " RENAME TO " + renamedViewName1);
        assertQueryFails("SELECT * FROM " + viewName1, ".*'iceberg.%s.%s' does not exist".formatted(LOWERCASE_SCHEMA, lowercaseViewName1));
        assertQuery("SELECT * FROM " + renamedViewName1, "VALUES (25, 99.4)");

        // Drop views
        assertUpdate("DROP VIEW " + renamedViewName1);
        assertUpdate("DROP VIEW " + viewName2);

        // Query dropped views
        assertQueryFails("SELECT * FROM " + renamedViewName1, ".*'iceberg.%s.%s' does not exist".formatted(LOWERCASE_SCHEMA, renamedViewName1.toLowerCase(ENGLISH)));
        assertQueryFails("SELECT * FROM " + viewName2, ".*'iceberg.%s.%s' does not exist".formatted(LOWERCASE_SCHEMA, lowercaseViewName2));
    }

    private String getColumnComment(String tableName, String columnName)
    {
        return (String) computeScalar("SELECT comment FROM information_schema.columns " +
                "WHERE table_schema = '" + LOWERCASE_SCHEMA + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'");
    }

    private static void createDir(String absoluteDirPath)
    {
        Path path = Paths.get(URI.create(absoluteDirPath).getPath());
        try {
            createDirectories(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Cannot create %s directory".formatted(absoluteDirPath), e);
        }
    }
}
