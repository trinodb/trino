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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestAddColumnTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testAddColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), new Identifier("new_col"), INTEGER, Optional.empty(), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("test", BIGINT), new ColumnMetadata("new_col", INTEGER));
    }

    @Test
    public void testAddColumnWithComment()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();

        getFutureValue(executeAddColumn(asQualifiedName(tableName), new Identifier("new_col"), INTEGER, Optional.of("test comment"), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(
                        new ColumnMetadata("test", BIGINT),
                        ColumnMetadata.builder()
                                .setName("new_col")
                                .setType(INTEGER)
                                .setComment(Optional.of("test comment"))
                                .build());
    }

    @Test
    public void testAddColumnWithColumnProperty()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        Property columnProperty = new Property(new Identifier("column_property"), new LongLiteral("111"));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), new Identifier("new_col"), INTEGER, ImmutableList.of(columnProperty), false, false));
        ColumnMetadata columnMetadata = metadata.getTableMetadata(testSession, table).getColumns().stream()
                .filter(column -> column.getName().equals("new_col"))
                .collect(onlyElement());
        assertThat(columnMetadata.getProperties()).containsExactly(Map.entry("column_property", 111L));
    }

    @Test
    public void testAddColumnNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), new Identifier("test"), INTEGER, Optional.empty(), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    public void testAddColumnNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeAddColumn(tableName, new Identifier("test"), INTEGER, Optional.empty(), true, false));
        // no exception
    }

    @Test
    public void testAddColumnNotExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        getFutureValue(executeAddColumn(asQualifiedName(tableName), new Identifier("test"), INTEGER, Optional.empty(), false, true));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("test", BIGINT));
    }

    @Test
    public void testAddColumnAlreadyExist()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(tableName), new Identifier("test"), INTEGER, Optional.empty(), false, false)))
                .hasErrorCode(COLUMN_ALREADY_EXISTS)
                .hasMessage("Column 'test' already exists");
    }

    @Test
    public void testAddColumnOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(viewName), new Identifier("test"), INTEGER, Optional.empty(), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", viewName);
    }

    @Test
    public void testAddColumnOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeAddColumn(asQualifiedName(materializedViewName), new Identifier("test"), INTEGER, Optional.empty(), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", materializedViewName);
    }

    private ListenableFuture<Void> executeAddColumn(QualifiedName table, Identifier column, Type type, Optional<String> comment, boolean tableExists, boolean columnNotExists)
    {
        ColumnDefinition columnDefinition = new ColumnDefinition(column, toSqlType(type), true, ImmutableList.of(), comment);
        return executeAddColumn(table, columnDefinition, tableExists, columnNotExists);
    }

    private ListenableFuture<Void> executeAddColumn(QualifiedName table, Identifier column, Type type, List<Property> properties, boolean tableExists, boolean columnNotExists)
    {
        ColumnDefinition columnDefinition = new ColumnDefinition(column, toSqlType(type), true, properties, Optional.empty());
        return executeAddColumn(table, columnDefinition, tableExists, columnNotExists);
    }

    private ListenableFuture<Void> executeAddColumn(QualifiedName table, ColumnDefinition columnDefinition, boolean tableExists, boolean columnNotExists)
    {
        return new AddColumnTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager)
                .execute(new AddColumn(table, columnDefinition, tableExists, columnNotExists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
