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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.ColumnPosition;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_DEFAULT_COLUMN_VALUE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.ConnectorCapabilities.DEFAULT_COLUMN_VALUE;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAddColumnTask
        extends BaseDataDefinitionTaskTest
{
    @Override
    @BeforeEach
    public void setUp()
    {
        super.setUp();
        metadata = new MockMetadataWithDefaultValue(TEST_CATALOG_NAME, ImmutableSet.of(DEFAULT_COLUMN_VALUE, NOT_NULL_COLUMN_CONSTRAINT));
        plannerContext = plannerContextBuilder().withMetadata(metadata).build();
    }

    @Test
    public void testAddColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("new_col"), INTEGER, Optional.empty(), new io.trino.sql.tree.ColumnPosition.Last(), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT), new ColumnMetadata("new_col", INTEGER));
    }

    @Test
    public void testAddColumnFirst()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("first_col"), INTEGER, Optional.empty(), new ColumnPosition.First(), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("first_col", INTEGER), new ColumnMetadata("test", BIGINT));
    }

    @Test
    public void testAddColumnAfter()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("last"), INTEGER, Optional.empty(), new ColumnPosition.Last(), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT), new ColumnMetadata("last", INTEGER));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("second"), VARCHAR, Optional.empty(), new ColumnPosition.After(new Identifier("test")), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT), new ColumnMetadata("second", VARCHAR), new ColumnMetadata("last", INTEGER));
    }

    @Test
    public void testAddColumnWithComment()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();

        getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("new_col"), INTEGER, Optional.of("test comment"), new ColumnPosition.Last(), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(
                        new ColumnMetadata("test", BIGINT),
                        ColumnMetadata.builder()
                                .setName("new_col")
                                .setType(INTEGER)
                                .setComment(Optional.of("test comment"))
                                .build());
    }

    @Test
    void testAddDefaultColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        ColumnDefinition column = new ColumnDefinition(
                new NodeLocation(1, 1),
                QualifiedName.of("new_col"),
                toSqlType(INTEGER),
                Optional.of(new LongLiteral(new NodeLocation(1, 1), "123")),
                true,
                ImmutableList.of(),
                Optional.empty());

        getFutureValue(executeAddColumn(asQualifiedName(tableName), column, new ColumnPosition.Last(), false, false));

        List<ColumnMetadata> columns = metadata.getTableMetadata(testSession, table).columns();
        assertThat(columns).hasSize(2);

        assertThat(columns.get(0).getName()).isEqualTo("test");

        assertThat(columns.get(1).getName()).isEqualTo("new_col");
        assertThat(columns.get(1).getDefaultValue()).contains("123");
    }

    @Test
    void testAddDefaultColumnTypeCoercion()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        ColumnDefinition column = new ColumnDefinition(
                new NodeLocation(1, 1),
                QualifiedName.of("new_col"),
                toSqlType(DOUBLE),
                Optional.of(new LongLiteral(new NodeLocation(1, 1), "123")),
                true,
                ImmutableList.of(),
                Optional.empty());

        getFutureValue(executeAddColumn(asQualifiedName(tableName), column, new ColumnPosition.Last(), false, false));

        List<ColumnMetadata> columns = metadata.getTableMetadata(testSession, table).columns();
        assertThat(columns).hasSize(2);

        assertThat(columns.get(0).getName()).isEqualTo("test");

        assertThat(columns.get(1).getName()).isEqualTo("new_col");
        assertThat(columns.get(1).getType()).isEqualTo(DOUBLE);
        assertThat(columns.get(1).getDefaultValue()).contains("123");
    }

    @Test
    void testAddDefaultColumnSupportedTypeCoercion()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        ColumnDefinition varcharColumn = new ColumnDefinition(
                new NodeLocation(1, 1),
                QualifiedName.of("new_varchar_col"),
                toSqlType(createVarcharType(4)),
                Optional.of(new StringLiteral(new NodeLocation(1, 1), "abcde")),
                true,
                ImmutableList.of(),
                Optional.empty());

        // MockMetadataWithDefaultValue.getSupportedType method changes varchar(4) to unbounded varchar
        getFutureValue(executeAddColumn(asQualifiedName(tableName), varcharColumn, new ColumnPosition.Last(), false, false));

        List<ColumnMetadata> columns = metadata.getTableMetadata(testSession, table).columns();
        assertThat(columns).hasSize(2);

        assertThat(columns.get(0).getName()).isEqualTo("test");

        assertThat(columns.get(1).getName()).isEqualTo("new_varchar_col");
        assertThat(columns.get(1).getType()).isEqualTo(VARCHAR);
        assertThat(columns.get(1).getDefaultValue()).contains("'abcde'");
    }

    @Test
    void testAddDefaultColumnUnsupportedTypeCoercion()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        ColumnDefinition charColumn = new ColumnDefinition(
                new NodeLocation(1, 1),
                QualifiedName.of("new_char_col"),
                toSqlType(createCharType(4)),
                Optional.of(new StringLiteral(new NodeLocation(1, 1), "abcde")),
                true,
                ImmutableList.of(),
                Optional.empty());

        assertTrinoExceptionThrownBy(() ->
                getFutureValue(executeAddColumn(asQualifiedName(tableName), charColumn, new ColumnPosition.Last(), false, false)))
                .hasErrorCode(INVALID_DEFAULT_COLUMN_VALUE)
                .hasMessage("line 1:1: ''abcde'' is not a valid CHAR(4) literal");
    }

    @Test
    void testAddDefaultNullAndNotNullColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        ColumnDefinition column = new ColumnDefinition(
                new NodeLocation(1, 1),
                QualifiedName.of("new_col"),
                toSqlType(INTEGER),
                Optional.of(new NullLiteral(new NodeLocation(1, 1))),
                false,
                ImmutableList.of(),
                Optional.empty());

        getFutureValue(executeAddColumn(asQualifiedName(tableName), column, new ColumnPosition.Last(), false, false));

        List<ColumnMetadata> columns = metadata.getTableMetadata(testSession, table).columns();
        assertThat(columns).hasSize(2);

        assertThat(columns.get(0).getName()).isEqualTo("test");

        assertThat(columns.get(1).getName()).isEqualTo("new_col");
        assertThat(columns.get(1).getDefaultValue()).contains("null");
    }

    @Test
    void testInvalidDefaultLiteral()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        ColumnDefinition column = new ColumnDefinition(
                new NodeLocation(1, 1),
                QualifiedName.of("new_col"),
                toSqlType(BIGINT),
                Optional.of(new StringLiteral(new NodeLocation(1, 1), "invalid")),
                true,
                ImmutableList.of(),
                Optional.empty());

        assertTrinoExceptionThrownBy(() ->
                getFutureValue(executeAddColumn(asQualifiedName(tableName), column, new ColumnPosition.Last(), false, false)))
                .hasErrorCode(INVALID_DEFAULT_COLUMN_VALUE)
                .hasMessage("line 1:1: ''invalid'' is not a valid BIGINT literal");
    }

    @Test
    void testAddDefaultColumnWithUnsupportedConnector()
    {
        Metadata metadata = new MockMetadataWithDefaultValue(TEST_CATALOG_NAME, ImmutableSet.of());
        PlannerContext plannerContext = plannerContextBuilder().withMetadata(metadata).build();

        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        ColumnDefinition column = new ColumnDefinition(
                new NodeLocation(1, 1),
                QualifiedName.of("new_col"),
                toSqlType(INTEGER),
                Optional.of(new LongLiteral(new NodeLocation(1, 1), "123")),
                true,
                ImmutableList.of(),
                Optional.empty());
        AddColumn addColumn = new AddColumn(new NodeLocation(1, 1), asQualifiedName(tableName), column, Optional.of(new ColumnPosition.Last()), false, false);

        assertTrinoExceptionThrownBy(() ->
                getFutureValue(new AddColumnTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager)
                        .execute(addColumn, queryStateMachine, ImmutableList.of(), WarningCollector.NOOP)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Catalog 'test_catalog' does not support default value for column name 'new_col'");
    }

    @Test
    public void testAddColumnWithColumnProperty()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        Property columnProperty = new Property(new Identifier("column_property"), new LongLiteral("111"));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("new_col"), INTEGER, ImmutableList.of(columnProperty), false, false));
        ColumnMetadata columnMetadata = metadata.getTableMetadata(testSession, table).columns().stream()
                .filter(column -> column.getName().equals("new_col"))
                .collect(onlyElement());
        assertThat(columnMetadata.getProperties()).containsExactly(Map.entry("column_property", 111L));
    }

    @Test
    public void testAddColumnNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("test"), INTEGER, Optional.empty(), new ColumnPosition.Last(), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    public void testAddColumnNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeAddColumn(tableName, QualifiedName.of("test"), INTEGER, Optional.empty(), new ColumnPosition.Last(), true, false));
        // no exception
    }

    @Test
    public void testAddColumnNotExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("test"), INTEGER, Optional.empty(), new ColumnPosition.Last(), false, true));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("test", BIGINT));
    }

    @Test
    public void testAddColumnAlreadyExist()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("test"), INTEGER, Optional.empty(), new ColumnPosition.Last(), false, false)))
                .hasErrorCode(COLUMN_ALREADY_EXISTS)
                .hasMessageContaining("Column 'test' already exists");
    }

    @Test
    public void testAddColumnOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(viewName), QualifiedName.of("test"), INTEGER, Optional.empty(), new ColumnPosition.Last(), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", viewName);
    }

    @Test
    public void testAddColumnOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(materializedViewName), QualifiedName.of("test"), INTEGER, Optional.empty(), new ColumnPosition.Last(), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", materializedViewName);
    }

    @Test
    public void testAddFieldWithNotExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, rowTable(tableName, new RowType.Field(Optional.of("a"), BIGINT)), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("col", rowType(new RowType.Field(Optional.of("a"), BIGINT))));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("col", "a"), INTEGER, false, true));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("col", rowType(new RowType.Field(Optional.of("a"), BIGINT))));
    }

    @Test
    public void testAddFieldToNotExistingField()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(
                testSession,
                TEST_CATALOG_NAME,
                rowTable(tableName, new RowType.Field(Optional.of("a"), rowType(new RowType.Field(Optional.of("b"), INTEGER)))),
                FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("col", "x", "c"), INTEGER, false, false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Field 'x' does not exist within row(a row(b integer))");
    }

    @Test
    public void testAddFieldToWithUnsupportedPosition()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(
                testSession,
                TEST_CATALOG_NAME,
                rowTable(tableName, new RowType.Field(Optional.of("a"), rowType(new RowType.Field(Optional.of("b"), INTEGER)))),
                FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(
                asQualifiedName(tableName),
                QualifiedName.of("col", "x", "c"),
                INTEGER,
                Optional.empty(),
                new ColumnPosition.First(),
                false,
                false)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Specifying column position is not supported for nested columns");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(
                asQualifiedName(tableName),
                QualifiedName.of("col", "x", "c"),
                INTEGER,
                Optional.empty(),
                new ColumnPosition.After(new Identifier("a")),
                false,
                false)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Specifying column position is not supported for nested columns");
    }

    @Test
    public void testUnsupportedMapTypeInRowField()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(
                testSession,
                TEST_CATALOG_NAME,
                rowTable(tableName, new RowType.Field(Optional.of("a"), new MapType(
                        rowType(new RowType.Field(Optional.of("key"), INTEGER)),
                        rowType(new RowType.Field(Optional.of("key"), INTEGER)),
                        new TypeOperators()))),
                FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("col", "a", "c"), INTEGER, false, false)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Unsupported type: map(row(key integer), row(key integer))");
    }

    @Test
    public void testUnsupportedAddDuplicatedField()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, rowTable(tableName, new RowType.Field(Optional.of("a"), BIGINT)), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("col", rowType(new RowType.Field(Optional.of("a"), BIGINT))));

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("col", "a"), INTEGER, false, false)))
                .hasErrorCode(COLUMN_ALREADY_EXISTS)
                .hasMessageContaining("Field 'a' already exists");
        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("col", "A"), INTEGER, false, false)))
                .hasErrorCode(COLUMN_ALREADY_EXISTS)
                .hasMessageContaining("Field 'a' already exists");
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("col", rowType(new RowType.Field(Optional.of("a"), BIGINT))));
    }

    @Test
    public void testUnsupportedAddAmbiguousField()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(
                testSession,
                TEST_CATALOG_NAME,
                rowTable(tableName,
                        new RowType.Field(Optional.of("a"), rowType(new RowType.Field(Optional.of("x"), INTEGER))),
                        new RowType.Field(Optional.of("A"), rowType(new RowType.Field(Optional.of("y"), INTEGER)))),
                FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("col", rowType(
                        new RowType.Field(Optional.of("a"), rowType(new RowType.Field(Optional.of("x"), INTEGER))),
                        new RowType.Field(Optional.of("A"), rowType(new RowType.Field(Optional.of("y"), INTEGER))))));

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), QualifiedName.of("col", "a", "z"), INTEGER, false, false)))
                .hasErrorCode(AMBIGUOUS_NAME)
                .hasMessageContaining("Field path [a, z] within row(a row(x integer), A row(y integer)) is ambiguous");
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(new ColumnMetadata("col", rowType(
                        new RowType.Field(Optional.of("a"), rowType(new RowType.Field(Optional.of("x"), INTEGER))),
                        new RowType.Field(Optional.of("A"), rowType(new RowType.Field(Optional.of("y"), INTEGER))))));
    }

    private ListenableFuture<Void> executeAddColumn(QualifiedName table, QualifiedName column, Type type, boolean tableExists, boolean columnNotExists)
    {
        return executeAddColumn(table, column, type, Optional.empty(), new ColumnPosition.Last(), tableExists, columnNotExists);
    }

    private ListenableFuture<Void> executeAddColumn(QualifiedName table, QualifiedName column, Type type, Optional<String> comment, ColumnPosition position, boolean tableExists, boolean columnNotExists)
    {
        ColumnDefinition columnDefinition = new ColumnDefinition(column, toSqlType(type), true, ImmutableList.of(), comment);
        return executeAddColumn(table, columnDefinition, position, tableExists, columnNotExists);
    }

    private ListenableFuture<Void> executeAddColumn(QualifiedName table, QualifiedName column, Type type, List<Property> properties, boolean tableExists, boolean columnNotExists)
    {
        ColumnDefinition columnDefinition = new ColumnDefinition(column, toSqlType(type), true, properties, Optional.empty());
        return executeAddColumn(table, columnDefinition, new ColumnPosition.Last(), tableExists, columnNotExists);
    }

    private ListenableFuture<Void> executeAddColumn(QualifiedName table, ColumnDefinition columnDefinition, ColumnPosition position, boolean tableExists, boolean columnNotExists)
    {
        return new AddColumnTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager)
                .execute(new AddColumn(new NodeLocation(1, 1), table, columnDefinition, Optional.of(position), tableExists, columnNotExists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private static ConnectorTableMetadata rowTable(QualifiedObjectName tableName, RowType.Field... fields)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(
                new ColumnMetadata("col", rowType(fields))));
    }

    private static class MockMetadataWithDefaultValue
            extends MockMetadata
    {
        private final Set<ConnectorCapabilities> capabilities;

        public MockMetadataWithDefaultValue(String catalogName, Set<ConnectorCapabilities> capabilities)
        {
            super(catalogName);
            this.capabilities = ImmutableSet.copyOf(capabilities);
        }

        @Override
        public Optional<Type> getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type)
        {
            if (type instanceof VarcharType) {
                return Optional.of(VARCHAR);
            }
            return Optional.empty();
        }

        @Override
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
        {
            return capabilities;
        }
    }
}
