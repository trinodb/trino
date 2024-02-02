
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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_MATERIALIZED_VIEW_PROPERTY;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
class TestCreateMaterializedViewTask
{
    private static final String DEFAULT_MATERIALIZED_VIEW_FOO_PROPERTY_VALUE = null;
    private static final Integer DEFAULT_MATERIALIZED_VIEW_BAR_PROPERTY_VALUE = 123;

    private static final ConnectorTableMetadata MOCK_TABLE = new ConnectorTableMetadata(
            new SchemaTableName("schema", "mock_table"),
            List.of(new ColumnMetadata("a", SMALLINT), new ColumnMetadata("b", BIGINT)),
            ImmutableMap.of("baz", "property_value"));

    private QueryRunner queryRunner;
    private MockMetadata metadata;
    private CreateMaterializedViewTask task;

    @BeforeEach
    void setUp()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .build());
        metadata = new MockMetadata();
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withMetadataWrapper(ignored -> metadata)
                .withGetMaterializedViewProperties(() -> ImmutableList.<PropertyMetadata<?>>builder()
                        .add(stringProperty("foo", "test materialized view property", DEFAULT_MATERIALIZED_VIEW_FOO_PROPERTY_VALUE, false))
                        .add(integerProperty("bar", "test materialized view property", DEFAULT_MATERIALIZED_VIEW_BAR_PROPERTY_VALUE, false))
                        .build())
                .build()));
        queryRunner.createCatalog(TEST_CATALOG_NAME, "mock", ImmutableMap.of());
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<Map<Class<? extends Statement>, DataDefinitionTask<?>>>() {}));
        task = (CreateMaterializedViewTask) tasks.get(CreateMaterializedView.class);
        this.queryRunner = queryRunner;
    }

    @AfterEach
    void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    @Test
    void testCreateMaterializedViewIfNotExists()
    {
        CreateMaterializedView statement = new CreateMaterializedView(
                Optional.empty(),
                QualifiedName.of("test_mv_if_not_exists"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of(TEST_CATALOG_NAME, "schema", "mock_table"))),
                false,
                true,
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            createMaterializedView(transactionSession, statement);
            assertThat(metadata.getCreateMaterializedViewCallCount()).isEqualTo(1);
            return null;
        });
    }

    @Test
    void testCreateMaterializedViewWithExistingView()
    {
        CreateMaterializedView statement = new CreateMaterializedView(
                Optional.empty(),
                QualifiedName.of("test_mv_with_existing_view"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of(TEST_CATALOG_NAME, "schema", "mock_table"))),
                false,
                false,
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() -> createMaterializedView(transactionSession, statement))
                    .hasErrorCode(ALREADY_EXISTS)
                    .hasMessage("Materialized view already exists");
            assertThat(metadata.getCreateMaterializedViewCallCount()).isEqualTo(1);
            return null;
        });
    }

    @Test
    void testCreateMaterializedViewWithInvalidProperty()
    {
        CreateMaterializedView statement = new CreateMaterializedView(
                Optional.empty(),
                QualifiedName.of("test_mv_with_invalid_property"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of(TEST_CATALOG_NAME, "schema", "mock_table"))),
                false,
                true,
                Optional.empty(),
                ImmutableList.of(new Property(new Identifier("baz"), new StringLiteral("abc"))),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() -> createMaterializedView(transactionSession, statement))
                    .hasErrorCode(INVALID_MATERIALIZED_VIEW_PROPERTY)
                    .hasMessage("Catalog 'test_catalog' materialized view property 'baz' does not exist");
            assertThat(metadata.getCreateMaterializedViewCallCount()).isEqualTo(0);
            return null;
        });
    }

    @Test
    void testCreateMaterializedViewWithDefaultProperties()
    {
        CreateMaterializedView statement = new CreateMaterializedView(
                Optional.empty(),
                QualifiedName.of(TEST_CATALOG_NAME, "schema", "mv_default_properties"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of(TEST_CATALOG_NAME, "schema", "mock_table"))),
                false,
                true,
                Optional.empty(),
                ImmutableList.of(
                        new Property(new Identifier("foo")),    // set foo to DEFAULT
                        new Property(new Identifier("bar"))),   // set bar to DEFAULT
                Optional.empty());
        queryRunner.inTransaction(transactionSession -> {
            createMaterializedView(transactionSession, statement);
            SchemaTableName viewName = SchemaTableName.schemaTableName("schema", "mv_default_properties");
            Optional<ConnectorMaterializedViewDefinition> definitionOptional = metadata.getMaterializedView(transactionSession.toConnectorSession(), viewName);
            assertThat(definitionOptional).isPresent();
            Map<String, Object> properties = metadata.getMaterializedViewProperties(transactionSession.toConnectorSession(), viewName, definitionOptional.get());
            assertThat(properties.get("foo")).isEqualTo(DEFAULT_MATERIALIZED_VIEW_FOO_PROPERTY_VALUE);
            assertThat(properties.get("bar")).isEqualTo(DEFAULT_MATERIALIZED_VIEW_BAR_PROPERTY_VALUE);
            return null;
        });
    }

    @Test
    public void testCreateDenyPermission()
    {
        CreateMaterializedView statement = new CreateMaterializedView(
                Optional.empty(),
                QualifiedName.of("test_mv_deny"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of(TEST_CATALOG_NAME, "schema", "mock_table"))),
                false,
                true,
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty());
        queryRunner.getAccessControl().deny(privilege("test_mv_deny", CREATE_MATERIALIZED_VIEW));

        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() -> createMaterializedView(transactionSession, statement))
                    .hasErrorCode(PERMISSION_DENIED)
                    .hasMessageContaining("Cannot create materialized view test_catalog.schema.test_mv");
            return null;
        });
    }

    private void createMaterializedView(Session transactionSession, CreateMaterializedView statement)
    {
        task.executeInternal(statement, transactionSession, ImmutableList.of(), WarningCollector.NOOP, createPlanOptimizersStatsCollector());
    }

    private static class MockMetadata
            implements ConnectorMetadata
    {
        private final Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new ConcurrentHashMap<>();
        private final Map<SchemaTableName, Map<String, Object>> materializedViewProperties = new ConcurrentHashMap<>();

        @Override
        public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, Map<String, Object> properties, boolean replace, boolean ignoreExisting)
        {
            materializedViews.put(viewName, definition);
            materializedViewProperties.put(viewName, properties);
            if (!ignoreExisting) {
                throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists");
            }
        }

        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
        {
            return MOCK_TABLE.getTableSchema();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
        {
            if (tableName.equals(MOCK_TABLE.getTable())) {
                return new TestingTableHandle(tableName);
            }
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return MOCK_TABLE.getColumns().stream()
                    .collect(toImmutableMap(
                            ColumnMetadata::getName,
                            column -> new TestingColumnHandle(column.getName())));
        }

        @Override
        public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
        {
            return Optional.ofNullable(materializedViews.get(viewName));
        }

        @Override
        public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition materializedViewDefinition)
        {
            return materializedViewProperties.get(viewName);
        }

        public int getCreateMaterializedViewCallCount()
        {
            return materializedViews.size();
        }
    }
}
