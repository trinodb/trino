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
import io.trino.connector.CatalogHandle;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SetDefaultValue;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_DEFAULT_COLUMN_VALUE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.ConnectorCapabilities.DEFAULT_COLUMN_VALUE;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSetDefaultValueTask
        extends BaseDataDefinitionTaskTest
{
    @Override
    @BeforeEach
    public void setUp()
    {
        super.setUp();
        metadata = new MockMetadataWithDefaultValue(TEST_CATALOG_NAME);
        plannerContext = plannerContextBuilder().withMetadata(metadata).build();
    }

    @Test
    void testSetDefaultValue()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");

        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(column("a", null), column("b", null));

        getFutureValue(executeSetDefaultValue(asQualifiedName(tableName), "b", "123", false));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(column("a", null), column("b", "123"));

        getFutureValue(executeSetDefaultValue(asQualifiedName(tableName), "b", "456", false));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(column("a", null), column("b", "456"));
    }

    @Test
    void testSetDefaultValueNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetDefaultValue(asQualifiedName(tableName), "b", "123", false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    void testSetDefaultValueNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeSetDefaultValue(tableName, "b", "123", true));
        // no exception
    }

    @Test
    void testSetWrongDefaultValueType()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetDefaultValue(
                asQualifiedName(tableName),
                QualifiedName.of("a"),
                new StringLiteral(new NodeLocation(1, 1), "test"),
                false)))
                .hasErrorCode(INVALID_DEFAULT_COLUMN_VALUE)
                .hasMessageContaining("line 1:1: ''test'' is not a valid BIGINT literal");
    }

    @Test
    void testSetDefaultFieldValue()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetDefaultValue(asQualifiedName(tableName), "missing_column", "123", false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column 'missing_column' does not exist");
    }

    @Test
    void testSetDefaultValueMissingColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetDefaultValue(
                asQualifiedName(tableName),
                QualifiedName.of("row", "field"),
                new LongLiteral(new NodeLocation(1, 1), "123"),
                false)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Cannot modify nested fields");
    }

    @Test
    void testSetDefaultValueOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetDefaultValue(asQualifiedName(viewName), "test", "123", false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a view with that name exists.", viewName);
    }

    @Test
    void testSetDefaultValueOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeSetDefaultValue(asQualifiedName(materializedViewName), "test", "123", false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a materialized view with that name exists.", materializedViewName);
    }

    private ListenableFuture<Void> executeSetDefaultValue(QualifiedName table, String column, String defaultValue, boolean tableExists)
    {
        return executeSetDefaultValue(table, QualifiedName.of(column), new LongLiteral(new NodeLocation(1, 1), defaultValue), tableExists);
    }

    private ListenableFuture<Void> executeSetDefaultValue(QualifiedName table, QualifiedName column, Literal defaultValue, boolean tableExists)
    {
        return new SetDefaultValueTask(plannerContext, new AllowAllAccessControl())
                .execute(new SetDefaultValue(
                                new NodeLocation(1, 1),
                                table,
                                column,
                                defaultValue,
                                tableExists),
                        queryStateMachine,
                        ImmutableList.of(),
                        WarningCollector.NOOP);
    }

    private static ConnectorTableMetadata simpleTable(QualifiedObjectName tableName)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(column("a", null), column("b", null)));
    }

    private static ColumnMetadata column(String name, String defaultValue)
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(BIGINT)
                .setDefaultValue(Optional.ofNullable(defaultValue))
                .build();
    }

    private static class MockMetadataWithDefaultValue
            extends MockMetadata
    {
        public MockMetadataWithDefaultValue(String catalogName)
        {
            super(catalogName);
        }

        @Override
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
        {
            return ImmutableSet.of(DEFAULT_COLUMN_VALUE);
        }
    }
}
