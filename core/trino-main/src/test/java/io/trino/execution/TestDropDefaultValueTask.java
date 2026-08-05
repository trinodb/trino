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
import io.trino.sql.tree.DropDefaultValue;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.ConnectorCapabilities.DEFAULT_COLUMN_VALUE;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDropDefaultValueTask
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
    void testDropDefaultValue()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");

        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTableWithDefault(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(column("a", "123"), column("b", "123"));

        getFutureValue(executeDropDefaultValue(asQualifiedName(tableName), "b", false));
        assertThat(metadata.getTableMetadata(testSession, table).columns())
                .containsExactly(column("a", "123"), column("b", null));
    }

    @Test
    void testDropDefaultValueNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropDefaultValue(asQualifiedName(tableName), "b", false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    void testDropDefaultValueNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeDropDefaultValue(tableName, "b", true));
        // no exception
    }

    @Test
    void testDropDefaultFieldValue()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTableWithDefault(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropDefaultValue(asQualifiedName(tableName), "missing_column", false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column 'missing_column' does not exist");
    }

    @Test
    void testDropDefaultValueMissingColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTableWithDefault(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropDefaultValue(asQualifiedName(tableName), QualifiedName.of("row", "field"), false)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Cannot modify nested fields");
    }

    @Test
    void testDropNonDefaultValueColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(column("a", null)));
        metadata.createTable(testSession, TEST_CATALOG_NAME, tableMetadata, FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropDefaultValue(asQualifiedName(tableName), "a", false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessageContaining("Column 'a' does not have a default value");
    }

    @Test
    void testDropDefaultValueOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropDefaultValue(asQualifiedName(viewName), "test", false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a view with that name exists.", viewName);
    }

    @Test
    void testDropDefaultValueOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropDefaultValue(asQualifiedName(materializedViewName), "test", false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a materialized view with that name exists.", materializedViewName);
    }

    private ListenableFuture<Void> executeDropDefaultValue(QualifiedName table, String column, boolean tableExists)
    {
        return executeDropDefaultValue(table, QualifiedName.of(column), tableExists);
    }

    private ListenableFuture<Void> executeDropDefaultValue(QualifiedName table, QualifiedName column, boolean tableExists)
    {
        return new DropDefaultValueTask(plannerContext.getMetadata(), new AllowAllAccessControl())
                .execute(new DropDefaultValue(
                        new NodeLocation(1, 1),
                        table,
                        column,
                        tableExists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private static ConnectorTableMetadata simpleTableWithDefault(QualifiedObjectName tableName)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(column("a", "123"), column("b", "123")));
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
