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
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SetColumnType;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestSetColumnTypeTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testSetDataType()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .isEqualTo(ImmutableList.of(new ColumnMetadata("test", BIGINT)));

        // Change the column type to integer from bigint
        getFutureValue(executeSetColumnType(asQualifiedName(tableName), QualifiedName.of("test"), toSqlType(INTEGER), false));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .isEqualTo(ImmutableList.of(new ColumnMetadata("test", INTEGER)));

        // Specify the same column type
        getFutureValue(executeSetColumnType(asQualifiedName(tableName), QualifiedName.of("test"), toSqlType(INTEGER), false));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .isEqualTo(ImmutableList.of(new ColumnMetadata("test", INTEGER)));
    }

    @Test
    public void testSetDataTypeNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(tableName), QualifiedName.of("test"), toSqlType(INTEGER), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    public void testSetDataTypeNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeSetColumnType(tableName, QualifiedName.of("test"), toSqlType(INTEGER), true));
        // no exception
    }

    @Test
    public void testSetDataTypeNotExistingColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        QualifiedName columnName = QualifiedName.of("not_existing_column");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(tableName), columnName, toSqlType(INTEGER), false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column '%s' does not exist", columnName);
    }

    @Test
    public void testSetDataTypeOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(viewName), QualifiedName.of("test"), toSqlType(INTEGER), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a view with that name exists.", viewName);
    }

    @Test
    public void testSetDataTypeOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(materializedViewName), QualifiedName.of("test"), toSqlType(INTEGER), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a materialized view with that name exists.", materializedViewName);
    }

    @Test
    public void testSetFieldDataTypeNotExistingColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, rowTable(tableName, new Field(Optional.of("a"), BIGINT)), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(tableName), QualifiedName.of("test", "a"), toSqlType(INTEGER), false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column 'test.a' does not exist");
    }

    @Test
    public void testSetFieldDataTypeNotExistingField()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, rowTable(tableName, new Field(Optional.of("a"), BIGINT)), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(tableName), QualifiedName.of("col", "b"), toSqlType(INTEGER), false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Field 'b' does not exist within row(a bigint)");
    }

    @Test
    public void testUnsupportedSetDataTypeDuplicatedField()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, rowTable(tableName, new RowType.Field(Optional.of("a"), BIGINT), new RowType.Field(Optional.of("a"), BIGINT)), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .isEqualTo(ImmutableList.of(new ColumnMetadata("col", RowType.rowType(
                        new RowType.Field(Optional.of("a"), BIGINT), new RowType.Field(Optional.of("a"), BIGINT)))));

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(tableName), QualifiedName.of("col", "a"), toSqlType(INTEGER), false)))
                .hasErrorCode(AMBIGUOUS_NAME)
                .hasMessageContaining("Field path [col, a] within row(a bigint, a bigint) is ambiguous");
    }

    private static ConnectorTableMetadata rowTable(QualifiedObjectName tableName, Field... fields)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(
                new ColumnMetadata("col", rowType(fields))));
    }

    private ListenableFuture<Void> executeSetColumnType(QualifiedName table, QualifiedName column, DataType type, boolean exists)
    {
        return new SetColumnTypeTask(metadata, plannerContext.getTypeManager(), new AllowAllAccessControl())
                .execute(new SetColumnType(new NodeLocation(1, 1), table, column, type, exists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
