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
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestDropColumnTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testDropColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("a", BIGINT), new ColumnMetadata("b", BIGINT));

        getFutureValue(executeDropColumn(asQualifiedName(tableName), QualifiedName.of("b"), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("a", BIGINT));
    }

    @Test
    public void testDropOnlyColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("test", BIGINT));

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropColumn(asQualifiedName(tableName), QualifiedName.of("test"), false, false)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Cannot drop the only column in a table");
    }

    @Test
    public void testDropColumnNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropColumn(asQualifiedName(tableName), QualifiedName.of("test"), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    public void testDropColumnNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeDropColumn(tableName, QualifiedName.of("test"), true, false));
        // no exception
    }

    @Test
    public void testDropMissingColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropColumn(asQualifiedName(tableName), QualifiedName.of("missing_column"), false, false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column 'missing_column' does not exist");
    }

    @Test
    public void testDropColumnIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();

        getFutureValue(executeDropColumn(asQualifiedName(tableName), QualifiedName.of("c"), false, true));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("a", BIGINT), new ColumnMetadata("b", BIGINT));
    }

    @Test
    public void testUnsupportedDropDuplicatedField()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, rowTable(tableName, new Field(Optional.of("a"), BIGINT), new Field(Optional.of("a"), BIGINT)), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .isEqualTo(ImmutableList.of(new ColumnMetadata("col", RowType.rowType(
                        new Field(Optional.of("a"), BIGINT), new Field(Optional.of("a"), BIGINT)))));

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropColumn(asQualifiedName(tableName), QualifiedName.of("col", "a"), false, false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Field path [a] within row(a bigint, a bigint) is ambiguous");
    }

    @Test
    public void testUnsupportedDropOnlyField()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, rowTable(tableName, new Field(Optional.of("a"), BIGINT)), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("col", RowType.rowType(new Field(Optional.of("a"), BIGINT))));

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropColumn(asQualifiedName(tableName), QualifiedName.of("col", "a"), false, false)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Cannot drop the only field in a row type");
    }

    @Test
    public void testDropColumnOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropColumn(asQualifiedName(viewName), QualifiedName.of("test"), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", viewName);
    }

    @Test
    public void testDropColumnOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropColumn(asQualifiedName(materializedViewName), QualifiedName.of("test"), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", materializedViewName);
    }

    private ListenableFuture<Void> executeDropColumn(QualifiedName table, QualifiedName column, boolean tableExists, boolean columnExists)
    {
        return new DropColumnTask(plannerContext.getMetadata(), new AllowAllAccessControl())
                .execute(new DropColumn(table, column, tableExists, columnExists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private static ConnectorTableMetadata simpleTable(QualifiedObjectName tableName)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(new ColumnMetadata("a", BIGINT), new ColumnMetadata("b", BIGINT)));
    }

    private static ConnectorTableMetadata rowTable(QualifiedObjectName tableName, Field... fields)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(
                new ColumnMetadata("col", RowType.rowType(fields))));
    }
}
