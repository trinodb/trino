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
package io.trino.plugin.paimon;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeId;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.ViewAlreadyExistException;
import org.apache.paimon.catalog.Catalog.ViewNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_METADATA_ERROR;
import static io.trino.plugin.paimon.PaimonSchemaProperties.OWNER_PROPERTY;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonMetadataViewTest
{
    private static final ConnectorSession SESSION = TestingConnectorSession.SESSION;
    private static final SchemaTableName VIEW_NAME = new SchemaTableName("test_schema", "test_view");

    @Test
    public void testGetViewRequiresTrinoDialect()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(view(Map.of("spark", "SELECT id FROM spark_table"))),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.getView(SESSION, VIEW_NAME))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon view 'test_schema.test_view' does not contain a Trino SQL dialect");
                });
    }

    @Test
    public void testGetViewRequiresNonBlankTrinoDialect()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(view(Map.of("trino", " "))),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.getView(SESSION, VIEW_NAME))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon view 'test_schema.test_view' does not contain a Trino SQL dialect");
                });
    }

    @Test
    public void testGetViewUsesTrinoDialect()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(view(Map.of(
                        "spark", "SELECT id FROM spark_table",
                        "trino", "SELECT id FROM trino_table"))),
                TESTING_TYPE_MANAGER);

        ConnectorViewDefinition view = metadata.getView(SESSION, VIEW_NAME).orElseThrow();

        assertThat(view.getOriginalSql()).isEqualTo("SELECT id FROM trino_table");
        assertThat(view.getCatalog()).isEmpty();
        assertThat(view.getSchema()).isEmpty();
        assertThat(view.getColumns()).hasSize(1);
        assertThat(view.getColumns().get(0).getName()).isEqualTo("id");
        assertThat(view.getColumns().get(0).getType()).isEqualTo(BIGINT.getTypeId());
        assertThat(view.getColumns().get(0).getComment()).contains("id column");
        assertThat(view.getOwner()).isEmpty();
    }

    @Test
    public void testGetViewReturnsOwnerFromPaimonOptions()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(view(
                        Map.of("trino", "SELECT id FROM trino_table"),
                        Map.of(OWNER_PROPERTY, "view_owner"))),
                TESTING_TYPE_MANAGER);

        ConnectorViewDefinition view = metadata.getView(SESSION, VIEW_NAME).orElseThrow();

        assertThat(view.getOwner()).contains("view_owner");
    }

    @Test
    public void testSystemSchemaViewReadsAreEmpty()
    {
        SystemSchemaRejectingViewCatalog catalog = new SystemSchemaRejectingViewCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        SchemaTableName systemView = new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options_view");

        assertThat(metadata.getView(SESSION, systemView)).isEmpty();
        assertThat(metadata.getViews(SESSION, Optional.of(SYSTEM_DATABASE_NAME))).isEmpty();
        assertThat(metadata.getViews(SESSION, Optional.empty())).isEmpty();
        assertThat(catalog.listDatabasesCalls).isEqualTo(1);
    }

    @Test
    public void testGetViewUsesTrinoFileSystemForFilesystemCatalogInitialization()
    {
        Options options = new Options();
        options.set(WAREHOUSE, "s3://bucket/warehouse");
        PaimonMetadata metadata = new PaimonMetadata(
                new PaimonCatalog(options, _ -> failingFileSystem()),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.getView(SESSION, VIEW_NAME))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception)
                            .hasMessageContaining("Failed to access Paimon warehouse 's3://bucket/warehouse' with Trino file system")
                            .hasMessageNotContaining("Hadoop configuration is not available")
                            .hasRootCauseMessage("simulated S3 probe failure");
                });
    }

    @Test
    public void testGetViewsReportsUnsupportedCatalog()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new UnsupportedListViewsPaimonCatalog(),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.getViews(SESSION, Optional.of(VIEW_NAME.getSchemaName())))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon catalog does not support view list operations");
                });
    }

    @Test
    public void testListTableColumnsReportsUnsupportedViewRead()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new UnsupportedGetViewPaimonCatalog(),
                TESTING_TYPE_MANAGER);
        SchemaTablePrefix prefix = new SchemaTablePrefix(VIEW_NAME.getSchemaName(), VIEW_NAME.getTableName());

        assertThatThrownBy(() -> metadata.listTableColumns(SESSION, prefix))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon catalog does not support view read operations");
                });

        Iterator<TableColumnsMetadata> streamedColumns = metadata.streamTableColumns(SESSION, prefix);
        assertThatThrownBy(streamedColumns::hasNext)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon catalog does not support view read operations");
                });
    }

    @Test
    public void testGetViewsSkipsViewsWithoutTrinoDialect()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(view(Map.of("spark", "SELECT id FROM spark_table"))),
                TESTING_TYPE_MANAGER);

        assertThat(metadata.getViews(SESSION, Optional.of(VIEW_NAME.getSchemaName()))).isEmpty();
    }

    @Test
    public void testListTableColumnsSkipsViewsWithoutTrinoDialect()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(view(Map.of("spark", "SELECT id FROM spark_table"))),
                TESTING_TYPE_MANAGER);

        assertThat(metadata.listTableColumns(SESSION, new SchemaTablePrefix(
                VIEW_NAME.getSchemaName(),
                VIEW_NAME.getTableName())))
                .isEmpty();

        Iterator<TableColumnsMetadata> streamedColumns = metadata.streamTableColumns(SESSION, new SchemaTablePrefix(
                VIEW_NAME.getSchemaName(),
                VIEW_NAME.getTableName()));
        assertThat(streamedColumns.hasNext()).isFalse();
    }

    @Test
    public void testListTableColumnsSkipsViewsWithBlankTrinoDialect()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(view(Map.of("trino", " "))),
                TESTING_TYPE_MANAGER);

        assertThat(metadata.listTableColumns(SESSION, new SchemaTablePrefix(
                VIEW_NAME.getSchemaName(),
                VIEW_NAME.getTableName())))
                .isEmpty();
    }

    @Test
    public void testListTableColumnsKeepsTrinoViewColumns()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(view(Map.of(
                        "spark", "SELECT id FROM spark_table",
                        "trino", "SELECT id FROM trino_table"))),
                TESTING_TYPE_MANAGER);

        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(SESSION, new SchemaTablePrefix(
                VIEW_NAME.getSchemaName(),
                VIEW_NAME.getTableName()));

        assertThat(columns).containsOnlyKeys(VIEW_NAME);
        assertThat(columns.get(VIEW_NAME))
                .singleElement()
                .satisfies(column -> {
                    assertThat(column.getName()).isEqualTo("id");
                    assertThat(column.getType()).isEqualTo(BIGINT);
                    assertThat(column.getComment()).contains("id column");
                });

        Iterator<TableColumnsMetadata> streamedColumns = metadata.streamTableColumns(SESSION, new SchemaTablePrefix(
                VIEW_NAME.getSchemaName(),
                VIEW_NAME.getTableName()));
        assertThat(streamedColumns.hasNext()).isTrue();
        TableColumnsMetadata viewColumns = streamedColumns.next();
        assertThat(viewColumns.getTable()).isEqualTo(VIEW_NAME);
        assertThat(viewColumns.getColumns().orElseThrow())
                .singleElement()
                .satisfies(column -> assertThat(column.getName()).isEqualTo("id"));
        assertThat(streamedColumns.hasNext()).isFalse();
    }

    @Test
    public void testGetViewsKeepsTrinoViewsWhenSkippingOtherDialects()
    {
        PaimonMetadata metadata = new PaimonMetadata(new MixedDialectViewCatalog(), TESTING_TYPE_MANAGER);

        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(SESSION, Optional.of("test_schema"));

        assertThat(views).containsOnlyKeys(new SchemaTableName("test_schema", "trino_view"));
        assertThat(views.get(new SchemaTableName("test_schema", "trino_view")).getOriginalSql())
                .isEqualTo("SELECT id FROM trino_table");
    }

    @Test
    public void testListViewsSkipsViewsWithoutTrinoDialect()
    {
        PaimonMetadata metadata = new PaimonMetadata(new MixedDialectViewCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.listViews(SESSION, Optional.of("test_schema")))
                .containsExactly(new SchemaTableName("test_schema", "trino_view"));
    }

    @Test
    public void testListViewsDoesNotReadViewColumnsForDialectFilter()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(viewWithUnreadableRowType(Map.of("trino", "SELECT id FROM trino_table"))),
                TESTING_TYPE_MANAGER);

        assertThat(metadata.listViews(SESSION, Optional.of(VIEW_NAME.getSchemaName())))
                .containsExactly(VIEW_NAME);
    }

    @Test
    public void testListViewsWithoutSchemaListsAllSchemas()
    {
        MultiSchemaViewCatalog catalog = new MultiSchemaViewCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        List<SchemaTableName> views = metadata.listViews(SESSION, Optional.empty());

        assertThat(catalog.listDatabasesCalls).isEqualTo(1);
        assertThat(catalog.listedSchemas).containsExactly("schema_a", "schema_b");
        assertThat(views).containsExactlyInAnyOrder(
                new SchemaTableName("schema_a", "view_a"),
                new SchemaTableName("schema_b", "view_b"));
    }

    @Test
    public void testGetViewsWithoutSchemaListsAllSchemas()
    {
        MultiSchemaViewCatalog catalog = new MultiSchemaViewCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(SESSION, Optional.empty());

        assertThat(catalog.listDatabasesCalls).isEqualTo(1);
        assertThat(catalog.listedSchemas).containsExactly("schema_a", "schema_b");
        assertThat(views.keySet()).containsExactlyInAnyOrder(
                new SchemaTableName("schema_a", "view_a"),
                new SchemaTableName("schema_b", "view_b"));
        assertThat(views.get(new SchemaTableName("schema_a", "view_a")).getOriginalSql())
                .isEqualTo("SELECT a_value");
        assertThat(views.get(new SchemaTableName("schema_b", "view_b")).getOriginalSql())
                .isEqualTo("SELECT b_value");
    }

    @Test
    public void testCreateViewUsesTypeManagerAndTrinoDialect()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(null);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                "SELECT payload FROM source_table",
                Optional.of("paimon"),
                Optional.of(VIEW_NAME.getSchemaName()),
                List.of(
                        new ConnectorViewDefinition.ViewColumn("payload", TypeId.of(JSON), Optional.of("json payload")),
                        new ConnectorViewDefinition.ViewColumn("name", VARCHAR.getTypeId(), Optional.empty())),
                Optional.of("view comment"),
                Optional.of("view_owner"),
                false,
                List.of(new CatalogSchemaName("paimon", VIEW_NAME.getSchemaName())));

        metadata.createView(SESSION, VIEW_NAME, definition, Map.of(), false);

        assertThat(catalog.dropViewCalls).isEqualTo(0);
        assertThat(catalog.createdIgnoreIfExists).isFalse();
        View createdView = catalog.createdView;
        assertThat(createdView).isNotNull();
        assertThat(createdView.dialects()).containsOnly(Map.entry("trino", "SELECT payload FROM source_table"));
        assertThat(createdView.query()).isEqualTo("SELECT payload FROM source_table");
        assertThat(createdView.comment()).contains("view comment");
        assertThat(createdView.options()).containsEntry("comment", "view comment");
        assertThat(createdView.options()).containsEntry(OWNER_PROPERTY, "view_owner");
        assertThat(createdView.rowType().getFields()).extracting(field -> field.type().getTypeRoot())
                .containsExactly(DataTypeRoot.VARIANT, DataTypeRoot.VARCHAR);
        assertThat(createdView.rowType().getFields()).extracting(field -> field.description())
                .containsExactly("json payload", null);
    }

    @Test
    public void testSystemSchemaViewWritesAreRejected()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT old_value")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        SchemaTableName systemView = new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options_view");

        assertThatThrownBy(() -> metadata.createView(SESSION, systemView, viewDefinition("SELECT value"), Map.of(), false))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon create view is not supported for the system schema 'sys'");
                });
        assertThatThrownBy(() -> metadata.dropView(SESSION, systemView))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon drop view is not supported for the system schema 'sys'");
                });
        assertThatThrownBy(() -> metadata.renameView(SESSION, VIEW_NAME, systemView))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon rename view is not supported for the system schema 'sys'");
                });
        assertThatThrownBy(() -> metadata.renameView(SESSION, systemView, VIEW_NAME))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon rename view is not supported for the system schema 'sys'");
                });
        assertThatThrownBy(() -> metadata.setViewComment(SESSION, systemView, Optional.of("comment")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon set view comment is not supported for the system schema 'sys'");
                });
        assertThatThrownBy(() -> metadata.setViewAuthorization(
                SESSION,
                systemView,
                new TrinoPrincipal(USER, "view_owner")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon set view authorization is not supported for the system schema 'sys'");
                });

        assertThat(catalog.createdView).isNull();
        assertThat(catalog.dropViewCalls).isZero();
        assertThat(catalog.renamedSource).isNull();
        assertThat(catalog.renamedTarget).isNull();
        assertThat(catalog.alterViewCalls).isZero();
    }

    @Test
    public void testViewEntrypointsValidateInputsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT old_value")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorViewDefinition definition = viewDefinition("SELECT value");

        assertThatThrownBy(() -> metadata.createView(null, VIEW_NAME, definition, Map.of(), false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.createView(SESSION, null, definition, Map.of(), false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("viewName is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.createView(SESSION, VIEW_NAME, null, Map.of(), false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("definition is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.dropView(null, VIEW_NAME))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.dropView(SESSION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("viewName is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.getView(null, VIEW_NAME))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.getView(SESSION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("viewName is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.renameView(
                null,
                VIEW_NAME,
                new SchemaTableName(VIEW_NAME.getSchemaName(), "renamed_view")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.renameView(
                SESSION,
                null,
                new SchemaTableName(VIEW_NAME.getSchemaName(), "renamed_view")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("source is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.renameView(SESSION, VIEW_NAME, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("target is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.getViews(null, Optional.of(VIEW_NAME.getSchemaName())))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.getViews(SESSION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("schemaName is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.getViews(SESSION, Optional.of(" ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.listViews(null, Optional.of(VIEW_NAME.getSchemaName())))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.listViews(SESSION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("schemaName is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.listViews(SESSION, Optional.of(" ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.setViewComment(null, VIEW_NAME, Optional.of("comment")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.setViewComment(SESSION, null, Optional.of("comment")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("viewName is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.setViewComment(SESSION, VIEW_NAME, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("comment is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.setViewAuthorization(
                null,
                VIEW_NAME,
                new TrinoPrincipal(USER, "view_owner")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.setViewAuthorization(
                SESSION,
                null,
                new TrinoPrincipal(USER, "view_owner")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("viewName is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.setViewAuthorization(SESSION, VIEW_NAME, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("principal is null");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testCreateOrReplaceViewDropsExistingViewBeforeCreating()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT old_value")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.createView(SESSION, VIEW_NAME, viewDefinition("SELECT new_value"), Map.of(), true);

        assertThat(catalog.dropViewCalls).isEqualTo(1);
        assertThat(catalog.droppedIgnoreIfNotExists).isTrue();
        assertThat(catalog.createdIgnoreIfExists).isFalse();
        assertThat(catalog.createdView.dialects()).containsOnly(Map.entry("trino", "SELECT new_value"));
    }

    @Test
    public void testCreateOrReplaceViewRestoresExistingViewWhenCreateFails()
    {
        View oldView = view(Map.of("trino", "SELECT old_value"));
        IOException failure = new IOException("view create metastore I/O failed");
        RestoreTrackingCreateViewFailureCatalog catalog = new RestoreTrackingCreateViewFailureCatalog(oldView, failure);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createView(SESSION, VIEW_NAME, viewDefinition("SELECT new_value"), Map.of(), true))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to create view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });

        assertThat(catalog.dropViewCalls).isEqualTo(1);
        assertThat(catalog.createAttempts).isEqualTo(2);
        assertThat(catalog.currentView).isSameAs(oldView);
    }

    @Test
    public void testCreateOrReplaceViewRestoreFailureIsSuppressedOnDeepMappedCause()
    {
        View oldView = view(Map.of("trino", "SELECT old_value"));
        Identifier identifier = new Identifier(VIEW_NAME.getSchemaName(), VIEW_NAME.getTableName());
        Catalog.ViewAlreadyExistException mappedFailure = new Catalog.ViewAlreadyExistException(identifier);
        RuntimeException failure = new RuntimeException(new RuntimeException(mappedFailure));
        RuntimeException restoreFailure = new RuntimeException("restore failed");
        RestoreFailingCreateViewFailureCatalog catalog = new RestoreFailingCreateViewFailureCatalog(oldView, failure, restoreFailure);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createView(SESSION, VIEW_NAME, viewDefinition("SELECT new_value"), Map.of(), true))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(ALREADY_EXISTS.toErrorCode());
                    assertThat(exception).hasMessage("View 'test_schema.test_view' already exists");
                    assertThat(exception.getCause()).isSameAs(mappedFailure);
                    assertThat(mappedFailure.getSuppressed()).containsExactly(restoreFailure);
                });

        assertThat(catalog.dropViewCalls).isEqualTo(1);
        assertThat(catalog.createAttempts).isEqualTo(2);
        assertThat(catalog.currentView).isNull();
    }

    @Test
    public void testDropViewSuccess()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT id FROM table")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.dropView(SESSION, VIEW_NAME);

        assertThat(catalog.dropViewCalls).isEqualTo(1);
        assertThat(catalog.droppedIgnoreIfNotExists).isFalse();
    }

    @Test
    public void testCreateViewRejectsExistingViewWithoutReplace()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT old_value")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createView(SESSION, VIEW_NAME, viewDefinition("SELECT new_value"), Map.of(), false))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(ALREADY_EXISTS.toErrorCode());
                    assertThat(exception).hasMessage("View 'test_schema.test_view' already exists");
                });

        assertThat(catalog.dropViewCalls).isEqualTo(0);
        assertThat(catalog.createdView).isNull();
    }

    @Test
    public void testRenameViewDelegatesToPaimonCatalog()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT old_value")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        SchemaTableName targetView = new SchemaTableName(VIEW_NAME.getSchemaName(), "renamed_view");

        metadata.renameView(SESSION, VIEW_NAME, targetView);

        assertThat(catalog.renamedSource).isEqualTo(new Identifier(VIEW_NAME.getSchemaName(), VIEW_NAME.getTableName()));
        assertThat(catalog.renamedTarget).isEqualTo(new Identifier(targetView.getSchemaName(), targetView.getTableName()));
        assertThat(catalog.renamedIgnoreIfNotExists).isFalse();
    }

    @Test
    public void testRenameViewTranslatesPaimonStateFailures()
    {
        SchemaTableName targetView = new SchemaTableName(VIEW_NAME.getSchemaName(), "renamed_view");

        PaimonMetadata missingSourceMetadata = new PaimonMetadata(
                new FailingRenameViewCatalog(new ViewNotExistException(
                        new Identifier(VIEW_NAME.getSchemaName(), VIEW_NAME.getTableName()))),
                TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> missingSourceMetadata.renameView(SESSION, VIEW_NAME, targetView))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(TABLE_NOT_FOUND.toErrorCode());
                    assertThat(exception).hasMessage("View 'test_schema.test_view' does not exist");
                });

        PaimonMetadata existingTargetMetadata = new PaimonMetadata(
                new FailingRenameViewCatalog(new ViewAlreadyExistException(
                        new Identifier(targetView.getSchemaName(), targetView.getTableName()))),
                TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> existingTargetMetadata.renameView(SESSION, VIEW_NAME, targetView))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(ALREADY_EXISTS.toErrorCode());
                    assertThat(exception).hasMessage("View 'test_schema.renamed_view' already exists");
                });
    }

    @Test
    public void testRuntimeWrappedRenameViewFailuresUseStandardErrors()
    {
        SchemaTableName targetView = new SchemaTableName(VIEW_NAME.getSchemaName(), "renamed_view");

        PaimonMetadata missingSourceMetadata = new PaimonMetadata(
                new RuntimeWrappedRenameViewFailureCatalog(new Catalog.ViewNotExistException(
                        new Identifier(VIEW_NAME.getSchemaName(), VIEW_NAME.getTableName()))),
                TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> missingSourceMetadata.renameView(SESSION, VIEW_NAME, targetView))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(TABLE_NOT_FOUND.toErrorCode());
                    assertThat(exception).hasMessage("View 'test_schema.test_view' does not exist");
                });

        PaimonMetadata existingTargetMetadata = new PaimonMetadata(
                new RuntimeWrappedRenameViewFailureCatalog(new Catalog.ViewAlreadyExistException(
                        new Identifier(targetView.getSchemaName(), targetView.getTableName()))),
                TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> existingTargetMetadata.renameView(SESSION, VIEW_NAME, targetView))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(ALREADY_EXISTS.toErrorCode());
                    assertThat(exception).hasMessage("View 'test_schema.renamed_view' already exists");
                });
    }

    @Test
    public void testRenameViewReportsUnsupportedCatalog()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new FailingRenameViewCatalog(new UnsupportedOperationException("views are not supported")),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.renameView(
                SESSION,
                VIEW_NAME,
                new SchemaTableName(VIEW_NAME.getSchemaName(), "renamed_view")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon catalog does not support view rename operations");
                });
    }

    @Test
    public void testCreateViewValidatesDefinitionBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(null);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                "SELECT value FROM source_table",
                Optional.empty(),
                Optional.empty(),
                List.of(new ConnectorViewDefinition.ViewColumn("value", TypeId.of("not_a_type"), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                false,
                List.of());

        assertThatThrownBy(() -> metadata.createView(SESSION, VIEW_NAME, definition, Map.of(), false))
                .hasMessageContaining("Unknown type: not_a_type");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testSetViewCommentSuccess()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT id FROM table")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.setViewComment(SESSION, VIEW_NAME, Optional.of("new comment"));

        assertThat(catalog.alterViewCalls).isEqualTo(1);
        assertThat(catalog.alterViewIgnoreIfNotExists).isFalse();
        assertThat(catalog.alterViewChanges).hasSize(1);
    }

    @Test
    public void testSetViewCommentClearsWithEmpty()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT id FROM table")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.setViewComment(SESSION, VIEW_NAME, Optional.empty());

        assertThat(catalog.alterViewCalls).isEqualTo(1);
        assertThat(catalog.alterViewChanges).hasSize(1);
    }

    @Test
    public void testSetViewAuthorizationStoresOwnerProperty()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(view(Map.of("trino", "SELECT id FROM table")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.setViewAuthorization(
                SESSION,
                VIEW_NAME,
                new TrinoPrincipal(USER, "new_owner"));

        assertThat(catalog.alterViewCalls).isEqualTo(1);
        assertThat(catalog.alterViewIgnoreIfNotExists).isFalse();
        assertThat(catalog.alterViewChanges)
                .singleElement()
                .isInstanceOfSatisfying(ViewChange.SetViewOption.class, change -> {
                    assertThat(change.key()).isEqualTo(OWNER_PROPERTY);
                    assertThat(change.value()).isEqualTo("new_owner");
                });
    }

    @Test
    public void testSetViewCommentNonExistentThrowsTableNotFound()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new FailingAlterViewCatalog(
                        new Catalog.ViewNotExistException(
                                new Identifier(VIEW_NAME.getSchemaName(), VIEW_NAME.getTableName()))),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.setViewComment(SESSION, VIEW_NAME, Optional.of("comment")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(TABLE_NOT_FOUND.toErrorCode());
                    assertThat(exception).hasMessage("View 'test_schema.test_view' does not exist");
                });
        assertThatThrownBy(() -> metadata.setViewAuthorization(
                SESSION,
                VIEW_NAME,
                new TrinoPrincipal(USER, "view_owner")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(TABLE_NOT_FOUND.toErrorCode());
                    assertThat(exception).hasMessage("View 'test_schema.test_view' does not exist");
                });
    }

    @Test
    public void testCreateViewInNonExistentSchemaThrowsSchemaNotFound()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new SchemaNotFoundViewCatalog(),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createView(SESSION, VIEW_NAME, viewDefinition("SELECT value"), Map.of(), false))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(SCHEMA_NOT_FOUND.toErrorCode());
                    assertThat(exception).hasMessage("Schema '%s' does not exist", VIEW_NAME.getSchemaName());
                });
    }

    @Test
    public void testRuntimeViewFailuresUsePaimonMetadataError()
    {
        IllegalStateException failure = new IllegalStateException("catalog invariant broken");
        PaimonMetadata metadata = new PaimonMetadata(new RuntimeFailingViewCatalog(failure), TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createView(SESSION, VIEW_NAME, viewDefinition("SELECT value"), Map.of(), false))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to create view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.dropView(SESSION, VIEW_NAME))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to drop view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.getView(SESSION, VIEW_NAME))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to get view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.getViews(SESSION, Optional.of(VIEW_NAME.getSchemaName())))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to list views in schema 'test_schema'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.renameView(
                SESSION,
                VIEW_NAME,
                new SchemaTableName(VIEW_NAME.getSchemaName(), "renamed_view")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to rename view 'test_schema.test_view' to 'test_schema.renamed_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.setViewComment(SESSION, VIEW_NAME, Optional.of("comment")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to set comment on view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.setViewAuthorization(
                SESSION,
                VIEW_NAME,
                new TrinoPrincipal(USER, "view_owner")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to set authorization on view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedViewFailuresUsePaimonMetadataError()
    {
        IOException failure = new IOException("metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingViewCatalog(failure), TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createView(SESSION, VIEW_NAME, viewDefinition("SELECT value"), Map.of(), false))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to create view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.dropView(SESSION, VIEW_NAME))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to drop view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.getView(SESSION, VIEW_NAME))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to get view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.getViews(SESSION, Optional.of(VIEW_NAME.getSchemaName())))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to list views in schema 'test_schema'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.renameView(
                SESSION,
                VIEW_NAME,
                new SchemaTableName(VIEW_NAME.getSchemaName(), "renamed_view")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to rename view 'test_schema.test_view' to 'test_schema.renamed_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.setViewComment(SESSION, VIEW_NAME, Optional.of("comment")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to set comment on view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.setViewAuthorization(
                SESSION,
                VIEW_NAME,
                new TrinoPrincipal(USER, "view_owner")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to set authorization on view 'test_schema.test_view'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    private static View view(Map<String, String> dialects)
    {
        return view(dialects, Map.of());
    }

    private static View view(Map<String, String> dialects, Map<String, String> options)
    {
        Identifier identifier = new Identifier(VIEW_NAME.getSchemaName(), VIEW_NAME.getTableName());
        return new ViewImpl(
                identifier,
                List.of(DataTypes.FIELD(0, "id", DataTypes.BIGINT(), "id column")),
                "SELECT id FROM canonical_table",
                dialects,
                null,
                options);
    }

    private static TrinoFileSystem failingFileSystem()
    {
        return (TrinoFileSystem) Proxy.newProxyInstance(
                PaimonMetadataViewTest.class.getClassLoader(),
                new Class<?>[] {TrinoFileSystem.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return handleObjectMethod(method.getName(), proxy, args);
                    }
                    if (method.getName().equals("newInputFile")) {
                        return failingInputFile((Location) args[0]);
                    }
                    if (method.getName().equals("directoryExists")) {
                        throw new IOException("simulated S3 probe failure");
                    }
                    throw new AssertionError("Unexpected filesystem call: " + method.getName());
                });
    }

    private static TrinoInputFile failingInputFile(Location location)
    {
        return (TrinoInputFile) Proxy.newProxyInstance(
                PaimonMetadataViewTest.class.getClassLoader(),
                new Class<?>[] {TrinoInputFile.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return handleObjectMethod(method.getName(), proxy, args);
                    }
                    if (method.getName().equals("exists")) {
                        throw new IOException("simulated S3 probe failure");
                    }
                    if (method.getName().equals("location")) {
                        return location;
                    }
                    throw new AssertionError("Unexpected input file call: " + method.getName());
                });
    }

    private static Object handleObjectMethod(String name, Object proxy, Object[] args)
    {
        return switch (name) {
            case "toString" -> proxy.getClass().getInterfaces()[0].getSimpleName() + " proxy";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new AssertionError("Unexpected Object method: " + name);
        };
    }

    private static View viewWithUnreadableRowType(Map<String, String> dialects)
    {
        Identifier identifier = new Identifier(VIEW_NAME.getSchemaName(), VIEW_NAME.getTableName());
        return new View()
        {
            @Override
            public String name()
            {
                return identifier.getObjectName();
            }

            @Override
            public String fullName()
            {
                return identifier.getFullName();
            }

            @Override
            public RowType rowType()
            {
                throw new AssertionError("listViews should not read view columns");
            }

            @Override
            public String query()
            {
                return "SELECT id FROM canonical_table";
            }

            @Override
            public Map<String, String> dialects()
            {
                return dialects;
            }

            @Override
            public Optional<String> comment()
            {
                return Optional.empty();
            }

            @Override
            public Map<String, String> options()
            {
                return Map.of();
            }

            @Override
            public View copy(Map<String, String> dynamicOptions)
            {
                return this;
            }
        };
    }

    private static ConnectorViewDefinition viewDefinition(String sql)
    {
        return new ConnectorViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                List.of(new ConnectorViewDefinition.ViewColumn("value", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                false,
                List.of());
    }

    private static class TestingPaimonCatalog
            extends PaimonCatalog
    {
        protected View currentView;
        private View createdView;
        private boolean initialized;
        private boolean createdIgnoreIfExists;
        protected int dropViewCalls;
        private boolean droppedIgnoreIfNotExists;
        private Identifier renamedSource;
        private Identifier renamedTarget;
        private boolean renamedIgnoreIfNotExists;
        private int alterViewCalls;
        private List<ViewChange> alterViewChanges;
        private boolean alterViewIgnoreIfNotExists;

        private TestingPaimonCatalog(View view)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.currentView = view;
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            initialized = true;
        }

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            initialized = true;
            return this;
        }

        @Override
        public View getView(Identifier identifier)
                throws Catalog.ViewNotExistException
        {
            assertThat(identifier.getDatabaseName()).isEqualTo(VIEW_NAME.getSchemaName());
            assertThat(identifier.getObjectName()).isEqualTo(VIEW_NAME.getTableName());
            if (currentView == null) {
                throw new Catalog.ViewNotExistException(identifier);
            }
            return currentView;
        }

        @Override
        public Table getTable(Identifier identifier)
                throws Catalog.TableNotExistException
        {
            throw new Catalog.TableNotExistException(identifier);
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            assertThat(databaseName).isEqualTo(VIEW_NAME.getSchemaName());
            return List.of(VIEW_NAME.getTableName());
        }

        @Override
        public void dropView(Identifier identifier, boolean ignoreIfNotExists)
                throws Catalog.ViewNotExistException
        {
            assertThat(identifier.getDatabaseName()).isEqualTo(VIEW_NAME.getSchemaName());
            assertThat(identifier.getObjectName()).isEqualTo(VIEW_NAME.getTableName());
            if (currentView == null && !ignoreIfNotExists) {
                throw new Catalog.ViewNotExistException(identifier);
            }
            dropViewCalls++;
            droppedIgnoreIfNotExists = ignoreIfNotExists;
            currentView = null;
        }

        @Override
        public void createView(Identifier identifier, View view, boolean ignoreIfExists)
                throws ViewAlreadyExistException
        {
            assertThat(identifier.getDatabaseName()).isEqualTo(VIEW_NAME.getSchemaName());
            assertThat(identifier.getObjectName()).isEqualTo(VIEW_NAME.getTableName());
            if (currentView != null && !ignoreIfExists) {
                throw new ViewAlreadyExistException(identifier);
            }
            createdView = view;
            currentView = view;
            createdIgnoreIfExists = ignoreIfExists;
        }

        @Override
        public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
                throws ViewAlreadyExistException,
                ViewNotExistException
        {
            renamedSource = fromView;
            renamedTarget = toView;
            renamedIgnoreIfNotExists = ignoreIfNotExists;
        }

        @Override
        public void alterView(
                Identifier identifier,
                List<ViewChange> viewChanges,
                boolean ignoreIfNotExists)
                throws Catalog.ViewNotExistException
        {
            assertThat(identifier.getDatabaseName()).isEqualTo(VIEW_NAME.getSchemaName());
            assertThat(identifier.getObjectName()).isEqualTo(VIEW_NAME.getTableName());
            alterViewCalls++;
            alterViewChanges = viewChanges;
            alterViewIgnoreIfNotExists = ignoreIfNotExists;
        }
    }

    private static class FailingRenameViewCatalog
            extends TestingPaimonCatalog
    {
        private final Exception failure;

        private FailingRenameViewCatalog(Exception failure)
        {
            super(null);
            this.failure = failure;
        }

        @Override
        public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
                throws ViewAlreadyExistException,
                ViewNotExistException
        {
            if (failure instanceof ViewAlreadyExistException e) {
                throw e;
            }
            if (failure instanceof ViewNotExistException e) {
                throw e;
            }
            if (failure instanceof RuntimeException e) {
                throw e;
            }
            throw new AssertionError("Unexpected checked rename failure", failure);
        }
    }

    private static class RestoreTrackingCreateViewFailureCatalog
            extends TestingPaimonCatalog
    {
        private final IOException failure;
        private int createAttempts;

        private RestoreTrackingCreateViewFailureCatalog(View view, IOException failure)
        {
            super(view);
            this.failure = failure;
        }

        @Override
        public void createView(Identifier identifier, View view, boolean ignoreIfExists)
                throws Catalog.ViewAlreadyExistException
        {
            createAttempts++;
            if (createAttempts == 1) {
                throw new RuntimeException(failure);
            }
            super.createView(identifier, view, ignoreIfExists);
        }
    }

    private static class RestoreFailingCreateViewFailureCatalog
            extends TestingPaimonCatalog
    {
        private final RuntimeException failure;
        private final RuntimeException restoreFailure;
        private int createAttempts;

        private RestoreFailingCreateViewFailureCatalog(View view, RuntimeException failure, RuntimeException restoreFailure)
        {
            super(view);
            this.failure = failure;
            this.restoreFailure = restoreFailure;
        }

        @Override
        public void createView(Identifier identifier, View view, boolean ignoreIfExists)
        {
            createAttempts++;
            if (createAttempts == 1) {
                throw failure;
            }
            throw restoreFailure;
        }
    }

    private static class UnsupportedListViewsPaimonCatalog
            extends TestingPaimonCatalog
    {
        private UnsupportedListViewsPaimonCatalog()
        {
            super(view(Map.of("trino", "SELECT id FROM trino_table")));
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            throw new UnsupportedOperationException("views are not supported");
        }
    }

    private static class UnsupportedGetViewPaimonCatalog
            extends TestingPaimonCatalog
    {
        private UnsupportedGetViewPaimonCatalog()
        {
            super(view(Map.of("trino", "SELECT id FROM trino_table")));
        }

        @Override
        public View getView(Identifier identifier)
        {
            throw new UnsupportedOperationException("views are not supported");
        }
    }

    private static class MixedDialectViewCatalog
            extends PaimonCatalog
    {
        private MixedDialectViewCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            assertThat(databaseName).isEqualTo(VIEW_NAME.getSchemaName());
            return List.of("spark_view", "trino_view");
        }

        @Override
        public View getView(Identifier identifier)
        {
            if (identifier.getObjectName().equals("spark_view")) {
                return view(Map.of("spark", "SELECT id FROM spark_table"));
            }
            if (identifier.getObjectName().equals("trino_view")) {
                return view(Map.of("trino", "SELECT id FROM trino_table"));
            }
            throw new AssertionError("Unexpected view: " + identifier.getFullName());
        }
    }

    private static class MultiSchemaViewCatalog
            extends PaimonCatalog
    {
        private int listDatabasesCalls;
        private final List<String> listedSchemas = new ArrayList<>();

        private MultiSchemaViewCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public List<String> listDatabases()
        {
            listDatabasesCalls++;
            return List.of("schema_a", "schema_b");
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            listedSchemas.add(databaseName);
            return switch (databaseName) {
                case "schema_a" -> List.of("view_a");
                case "schema_b" -> List.of("view_b");
                default -> throw new AssertionError("Unexpected schema: " + databaseName);
            };
        }

        @Override
        public View getView(Identifier identifier)
        {
            if (identifier.getFullName().equals("schema_a.view_a")) {
                return view(identifier, "SELECT a_value");
            }
            if (identifier.getFullName().equals("schema_b.view_b")) {
                return view(identifier, "SELECT b_value");
            }
            throw new AssertionError("Unexpected view: " + identifier.getFullName());
        }

        private static View view(Identifier identifier, String sql)
        {
            return new ViewImpl(
                    identifier,
                    List.of(DataTypes.FIELD(0, "id", DataTypes.BIGINT())),
                    sql,
                    Map.of("trino", sql),
                    null,
                    Map.of());
        }
    }

    private static class SystemSchemaRejectingViewCatalog
            extends PaimonCatalog
    {
        private int listDatabasesCalls;

        private SystemSchemaRejectingViewCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public List<String> listDatabases()
        {
            listDatabasesCalls++;
            return List.of(SYSTEM_DATABASE_NAME);
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            throw new AssertionError("system schema views must be empty without querying the catalog");
        }

        @Override
        public View getView(Identifier identifier)
        {
            throw new AssertionError("system schema view lookup must not query the catalog");
        }
    }

    private static class RuntimeWrappedRenameViewFailureCatalog
            extends TestingPaimonCatalog
    {
        private final Exception failure;

        private RuntimeWrappedRenameViewFailureCatalog(Exception failure)
        {
            super(null);
            this.failure = failure;
        }

        @Override
        public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
        {
            assertThat(fromView.getFullName()).isEqualTo(VIEW_NAME.toString());
            assertThat(toView.getFullName()).isEqualTo(VIEW_NAME.getSchemaName() + ".renamed_view");
            assertThat(ignoreIfNotExists).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class RuntimeFailingViewCatalog
            extends TestingPaimonCatalog
    {
        private final RuntimeException failure;

        private RuntimeFailingViewCatalog(RuntimeException failure)
        {
            super(null);
            this.failure = failure;
        }

        @Override
        public View getView(Identifier identifier)
        {
            throw failure;
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            throw failure;
        }

        @Override
        public void dropView(Identifier identifier, boolean ignoreIfNotExists)
        {
            throw failure;
        }

        @Override
        public void createView(Identifier identifier, View view, boolean ignoreIfExists)
        {
            throw failure;
        }

        @Override
        public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
        {
            throw failure;
        }

        @Override
        public void alterView(
                Identifier identifier,
                List<ViewChange> viewChanges,
                boolean ignoreIfNotExists)
        {
            throw failure;
        }
    }

    private static class FailingAlterViewCatalog
            extends TestingPaimonCatalog
    {
        private final Exception failure;

        private FailingAlterViewCatalog(Exception failure)
        {
            super(view(Map.of("trino", "SELECT id FROM table")));
            this.failure = failure;
        }

        @Override
        public void alterView(
                Identifier identifier,
                List<ViewChange> viewChanges,
                boolean ignoreIfNotExists)
                throws Catalog.ViewNotExistException
        {
            if (failure instanceof Catalog.ViewNotExistException e) {
                throw e;
            }
            if (failure instanceof RuntimeException e) {
                throw e;
            }
            throw new AssertionError("Unexpected failure", failure);
        }
    }

    private static class CheckedFailingViewCatalog
            extends TestingPaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingViewCatalog(IOException failure)
        {
            super(view(Map.of("trino", "SELECT id FROM table")));
            this.failure = failure;
        }

        @Override
        public View getView(Identifier identifier)
        {
            throw new RuntimeException(failure);
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            throw new RuntimeException(failure);
        }

        @Override
        public void dropView(Identifier identifier, boolean ignoreIfNotExists)
        {
            throw new RuntimeException(failure);
        }

        @Override
        public void createView(Identifier identifier, View view, boolean ignoreIfExists)
        {
            throw new RuntimeException(failure);
        }

        @Override
        public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
        {
            throw new RuntimeException(failure);
        }

        @Override
        public void alterView(
                Identifier identifier,
                List<ViewChange> viewChanges,
                boolean ignoreIfNotExists)
        {
            throw new RuntimeException(failure);
        }
    }

    private static class SchemaNotFoundViewCatalog
            extends PaimonCatalog
    {
        private SchemaNotFoundViewCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void createView(Identifier identifier, View view, boolean ignoreIfExists)
                throws Catalog.DatabaseNotExistException
        {
            throw new Catalog.DatabaseNotExistException(identifier.getDatabaseName());
        }
    }

    private static TrinoFileSystemFactory unsupportedFileSystemFactory()
    {
        return _ -> {
            throw new UnsupportedOperationException("filesystem is not used by this test");
        };
    }
}
