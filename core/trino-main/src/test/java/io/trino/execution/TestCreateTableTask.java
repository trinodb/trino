
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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TableElement;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.tree.LikeClause.PropertiesOption.INCLUDING;
import static io.trino.sql.tree.SaveMode.FAIL;
import static io.trino.sql.tree.SaveMode.IGNORE;
import static io.trino.sql.tree.SaveMode.REPLACE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_CREATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.emptyList;
import static java.util.Locale.ROOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
class TestCreateTableTask
{
    private static final String OTHER_CATALOG_NAME = "other_catalog";
    private static final ConnectorTableMetadata PARENT_TABLE = new ConnectorTableMetadata(
            new SchemaTableName("schema", "parent_table"),
            List.of(new ColumnMetadata("a", SMALLINT), new ColumnMetadata("b", BIGINT)),
            Map.of("baz", "property_value"));

    private static final ConnectorTableMetadata PARENT_TABLE_WITH_COERCED_TYPE = new ConnectorTableMetadata(
            new SchemaTableName("schema", "parent_table_with_coerced_type"),
            List.of(new ColumnMetadata("a", TIMESTAMP_NANOS)));

    private QueryRunner queryRunner;
    private MockMetadata metadata;
    private CreateTableTask createTableTask;

    @BeforeEach
    void setUp()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .build());
        metadata = new MockMetadata();
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withMetadataWrapper(ignored -> metadata)
                .withTableProperties(() -> ImmutableList.of(stringProperty("baz", "test property", null, false)))
                .withCapabilities(() -> ImmutableSet.of(ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT))
                .build()));
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withMetadataWrapper(ignored -> metadata)
                .withName("other_mock")
                .build()));

        queryRunner.createCatalog(TEST_CATALOG_NAME, "mock", ImmutableMap.of());
        queryRunner.createCatalog(
                OTHER_CATALOG_NAME,
                "other_mock",
                ImmutableMap.of());

        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<Map<Class<? extends Statement>, DataDefinitionTask<?>>>() {}));
        createTableTask = (CreateTableTask) tasks.get(CreateTable.class);
        this.queryRunner = queryRunner;
    }

    @AfterEach
    void tearDown()
            throws IOException
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    @Test
    void testCreateTableNotExistsTrue()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table_if_not_exists"),
                ImmutableList.of(new ColumnDefinition(QualifiedName.of("a"), toSqlType(BIGINT), true, emptyList(), Optional.empty())),
                IGNORE,
                ImmutableList.of(),
                Optional.empty());
        queryRunner.inTransaction(transactionSession -> {
            getFutureValue(createTableTask.internalExecute(statement, transactionSession, emptyList(), output -> {}));
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(1);
            return null;
        });
    }

    @Test
    void testCreateTableNotExistsFalse()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table_fail_if_exists"),
                ImmutableList.of(new ColumnDefinition(QualifiedName.of("a"), toSqlType(BIGINT), true, emptyList(), Optional.empty())),
                FAIL,
                ImmutableList.of(),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() -> getFutureValue(createTableTask.internalExecute(statement, transactionSession, emptyList(), output -> {})))
                    .hasErrorCode(ALREADY_EXISTS)
                    .hasMessage("Table already exists");
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(1);
            return null;
        });
    }

    @Test
    void testReplaceTable()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table_replace"),
                ImmutableList.of(new ColumnDefinition(QualifiedName.of("a"), toSqlType(BIGINT), true, emptyList(), Optional.empty())),
                REPLACE,
                ImmutableList.of(),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            getFutureValue(createTableTask.internalExecute(statement, transactionSession, emptyList(), output -> {}));
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(1);
            assertThat(metadata.getReceivedTableMetadata().get(0).getColumns())
                    .isEqualTo(ImmutableList.of(new ColumnMetadata("a", BIGINT)));
            return null;
        });
    }

    @Test
    void testCreateTableWithMaterializedViewPropertyFails()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table_with_materialized_view_property"),
                ImmutableList.of(new ColumnDefinition(QualifiedName.of("a"), toSqlType(BIGINT), true, emptyList(), Optional.empty())),
                FAIL,
                ImmutableList.of(new Property(new Identifier("foo"), new StringLiteral("bar"))),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() -> getFutureValue(createTableTask.internalExecute(statement, transactionSession, emptyList(), output -> {})))
                    .hasErrorCode(INVALID_TABLE_PROPERTY)
                    .hasMessage("Catalog 'test_catalog' table property 'foo' does not exist");
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(0);
            return null;
        });
    }

    @Test
    void testCreateWithNotNullColumns()
    {
        List<TableElement> inputColumns = ImmutableList.of(
                new ColumnDefinition(QualifiedName.of("a"), toSqlType(DATE), true, emptyList(), Optional.empty()),
                new ColumnDefinition(QualifiedName.of("b"), toSqlType(VARCHAR), false, emptyList(), Optional.empty()),
                new ColumnDefinition(QualifiedName.of("c"), toSqlType(VARBINARY), false, emptyList(), Optional.empty()));
        CreateTable statement = new CreateTable(QualifiedName.of("test_table_not_null_columns"), inputColumns, IGNORE, ImmutableList.of(), Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            getFutureValue(createTableTask.internalExecute(statement, transactionSession, emptyList(), output -> {}));
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(1);
            List<ColumnMetadata> columns = metadata.getReceivedTableMetadata().get(0).getColumns();
            assertThat(columns.size()).isEqualTo(3);

            assertThat(columns.get(0).getName()).isEqualTo("a");
            assertThat(columns.get(0).getType().getDisplayName().toUpperCase(ROOT)).isEqualTo("DATE");
            assertThat(columns.get(0).isNullable()).isTrue();

            assertThat(columns.get(1).getName()).isEqualTo("b");
            assertThat(columns.get(1).getType().getDisplayName().toUpperCase(ROOT)).isEqualTo("VARCHAR");
            assertThat(columns.get(1).isNullable()).isFalse();

            assertThat(columns.get(2).getName()).isEqualTo("c");
            assertThat(columns.get(2).getType().getDisplayName().toUpperCase(ROOT)).isEqualTo("VARBINARY");
            assertThat(columns.get(2).isNullable()).isFalse();
            return null;
        });
    }

    @Test
    void testCreateWithUnsupportedConnectorThrowsWhenNotNull()
    {
        List<TableElement> inputColumns = ImmutableList.of(
                new ColumnDefinition(QualifiedName.of("a"), toSqlType(DATE), true, emptyList(), Optional.empty()),
                new ColumnDefinition(QualifiedName.of("b"), toSqlType(VARCHAR), false, emptyList(), Optional.empty()),
                new ColumnDefinition(QualifiedName.of("c"), toSqlType(VARBINARY), false, emptyList(), Optional.empty()));
        CreateTable statement = new CreateTable(
                QualifiedName.of(OTHER_CATALOG_NAME, "other_schema", "test_table_unsupported_connector"),
                inputColumns,
                IGNORE,
                ImmutableList.of(),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() ->
                    getFutureValue(createTableTask.internalExecute(statement, transactionSession, emptyList(), output -> {})))
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("Catalog 'other_catalog' does not support non-null column for column name 'b'");
            return null;
        });
    }

    @Test
    void testCreateLike()
    {
        CreateTable statement = getCreateLikeStatement(false);

        queryRunner.inTransaction(transactionSession -> {
            getFutureValue(createTableTask.internalExecute(statement, transactionSession, List.of(), output -> {}));
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(1);

            assertThat(metadata.getReceivedTableMetadata().get(0).getColumns())
                    .isEqualTo(PARENT_TABLE.getColumns());
            assertThat(metadata.getReceivedTableMetadata().get(0).getProperties()).isEmpty();
            return null;
        });
    }

    @Test
    void testCreateLikeIncludingProperties()
    {
        CreateTable statement = getCreateLikeStatement(true);

        queryRunner.inTransaction(transactionSession -> {
            getFutureValue(createTableTask.internalExecute(statement, transactionSession, List.of(), output -> {}));
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(1);

            assertThat(metadata.getReceivedTableMetadata().get(0).getColumns())
                    .isEqualTo(PARENT_TABLE.getColumns());
            assertThat(metadata.getReceivedTableMetadata().get(0).getProperties())
                    .isEqualTo(PARENT_TABLE.getProperties());
            return null;
        });
    }

    @Test
    void testCreateLikeExcludingPropertiesAcrossCatalogs()
    {
        CreateTable statement = getCreateLikeStatement(QualifiedName.of(OTHER_CATALOG_NAME, "other_schema", "test_table_excluding"), false);

        queryRunner.inTransaction(transactionSession -> {
            getFutureValue(createTableTask.internalExecute(statement, transactionSession, List.of(), output -> {}));
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(1);

            assertThat(metadata.getReceivedTableMetadata().get(0).getColumns())
                    .isEqualTo(PARENT_TABLE.getColumns());
            return null;
        });
    }

    @Test
    void testCreateLikeIncludingPropertiesAcrossCatalogs()
    {
        CreateTable failingStatement = getCreateLikeStatement(QualifiedName.of(OTHER_CATALOG_NAME, "other_schema", "test_table_including"), true);

        queryRunner.inTransaction(transactionSession -> {
            assertThatThrownBy(() -> getFutureValue(createTableTask.internalExecute(failingStatement, transactionSession, List.of(), output -> {})))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("CREATE TABLE LIKE table INCLUDING PROPERTIES across catalogs is not supported");
            return null;
        });
    }

    @Test
    void testCreateLikeDenyPermission()
    {
        CreateTable statement = getCreateLikeStatement(false);
        queryRunner.getAccessControl().deny(privilege("parent_table", SELECT_COLUMN));
        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() -> getFutureValue(createTableTask.internalExecute(statement, transactionSession, List.of(), output -> {})))
                    .hasErrorCode(PERMISSION_DENIED)
                    .hasMessageContaining("Cannot reference columns of table");
            return null;
        });
        queryRunner.getAccessControl().reset();
    }

    @Test
    void testCreateLikeIncludingPropertiesDenyPermission()
    {
        CreateTable statement = getCreateLikeStatement(true);

        queryRunner.getAccessControl().deny(privilege("parent_table", SHOW_CREATE_TABLE));
        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() -> getFutureValue(createTableTask.internalExecute(statement, transactionSession, List.of(), output -> {})))
                    .hasErrorCode(PERMISSION_DENIED)
                    .hasMessageContaining("Cannot reference properties of table");
            return null;
        });
        queryRunner.getAccessControl().reset();
    }

    @Test
    void testUnsupportedCreateTableWithField()
    {
        CreateTable statement = new CreateTable(
                QualifiedName.of("test_table_unsupported_field_123"),
                ImmutableList.of(new ColumnDefinition(QualifiedName.of("a", "b"), toSqlType(DATE), true, emptyList(), Optional.empty())),
                FAIL,
                ImmutableList.of(),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            assertTrinoExceptionThrownBy(() ->
                    getFutureValue(createTableTask.internalExecute(statement, transactionSession, emptyList(), output -> {})))
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("Column name 'a.b' must not be qualified");
            return null;
        });
    }

    @Test
    void testCreateTableWithCoercedType()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table_coerced_type"),
                ImmutableList.of(
                        new ColumnDefinition(
                                QualifiedName.of("a"),
                                toSqlType(TIMESTAMP_NANOS),
                                true,
                                emptyList(),
                                Optional.empty())),
                IGNORE,
                ImmutableList.of(),
                Optional.empty());
        queryRunner.inTransaction(transactionSession -> {
            getFutureValue(createTableTask.internalExecute(statement, transactionSession, List.of(), output -> {}));
            assertThat(metadata.getReceivedTableMetadata().get(0).getColumns().get(0).getType()).isEqualTo(TIMESTAMP_MILLIS);
            return null;
        });
    }

    @Test
    void testCreateTableLikeWithCoercedType()
    {
        CreateTable statement = new CreateTable(
                QualifiedName.of("test_table_like_coerced_type"),
                List.of(
                        new LikeClause(
                                QualifiedName.of(PARENT_TABLE_WITH_COERCED_TYPE.getTable().getTableName()),
                                Optional.of(INCLUDING))),
                IGNORE,
                ImmutableList.of(),
                Optional.empty());

        queryRunner.inTransaction(transactionSession -> {
            getFutureValue(createTableTask.internalExecute(statement, transactionSession, List.of(), output -> {}));
            assertThat(metadata.getCreateTableCallCount()).isEqualTo(1);

            assertThat(metadata.getReceivedTableMetadata().get(0).getColumns())
                    .isEqualTo(ImmutableList.of(new ColumnMetadata("a", TIMESTAMP_MILLIS)));
            assertThat(metadata.getReceivedTableMetadata().get(0).getProperties()).isEmpty();
            return null;
        });
    }

    private static CreateTable getCreateLikeStatement(boolean includingProperties)
    {
        return getCreateLikeStatement(QualifiedName.of("test_table_" + ThreadLocalRandom.current().longs(Long.MAX_VALUE)), includingProperties);
    }

    private static CreateTable getCreateLikeStatement(QualifiedName name, boolean includingProperties)
    {
        return new CreateTable(
                name,
                List.of(new LikeClause(QualifiedName.of(PARENT_TABLE.getTable().getTableName()), includingProperties ? Optional.of(INCLUDING) : Optional.empty())),
                IGNORE,
                ImmutableList.of(),
                Optional.empty());
    }

    private static class MockMetadata
            implements ConnectorMetadata
    {
        private final List<ConnectorTableMetadata> tables = new CopyOnWriteArrayList<>();

        @Override
        public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
        {
            if (saveMode == SaveMode.REPLACE) {
                tables.removeIf(table -> table.getTable().equals(tableMetadata.getTable()));
            }
            tables.add(tableMetadata);
            if (saveMode == SaveMode.FAIL) {
                throw new TrinoException(ALREADY_EXISTS, "Table already exists");
            }
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
        {
            if (tableName.equals(PARENT_TABLE.getTable()) || tableName.equals(PARENT_TABLE_WITH_COERCED_TYPE.getTable())) {
                return new TestingTableHandle(tableName);
            }
            return null;
        }

        @Override
        public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, Type type)
        {
            if (type instanceof TimestampType) {
                return Optional.of(TIMESTAMP_MILLIS);
            }
            return Optional.empty();
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            if ((tableHandle instanceof TestingTableHandle handle)) {
                if (handle.getTableName().equals(PARENT_TABLE.getTable())) {
                    return PARENT_TABLE;
                }
                if (handle.getTableName().equals(PARENT_TABLE_WITH_COERCED_TYPE.getTable())) {
                    return PARENT_TABLE_WITH_COERCED_TYPE;
                }
            }
            return ConnectorMetadata.super.getTableMetadata(session, tableHandle);
        }

        public int getCreateTableCallCount()
        {
            return tables.size();
        }

        public List<ConnectorTableMetadata> getReceivedTableMetadata()
        {
            return tables;
        }
    }
}
