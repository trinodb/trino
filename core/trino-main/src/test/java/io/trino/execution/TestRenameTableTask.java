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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.RenameTable;
import org.junit.jupiter.api.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRenameTableTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testRenameExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        QualifiedObjectName newTableName = qualifiedObjectName("existing_view_new");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(executeRenameTable(asQualifiedName(tableName), asQualifiedName(newTableName), false));
        assertThat(metadata.getTableHandle(testSession, tableName)).isEmpty();
        assertThat(metadata.getTableHandle(testSession, newTableName)).isPresent();
    }

    @Test
    public void testRenameNotExistingTable()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(tableName, qualifiedName("not_existing_table_new"), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    public void testRenameNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeRenameTable(tableName, qualifiedName("not_existing_table_new"), true));
        // no exception
    }

    @Test
    public void testRenameTableOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(asQualifiedName(viewName), qualifiedName("existing_view_new"), false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessageContaining("Table '%s' does not exist, but a view with that name exists. Did you mean ALTER VIEW %s RENAME TO ...?", viewName, viewName);
    }

    @Test
    public void testRenameTableOnViewIfExists()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(asQualifiedName(viewName), qualifiedName("existing_view_new"), true)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessageContaining("Table '%s' does not exist, but a view with that name exists. Did you mean ALTER VIEW %s RENAME TO ...?", viewName, viewName);
    }

    @Test
    public void testRenameTableOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(viewName, qualifiedName("existing_materialized_view_new"), false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessageContaining("Table '%s' does not exist, but a materialized view with that name exists. Did you mean ALTER MATERIALIZED VIEW test_catalog.schema.existing_materialized_view RENAME TO ...?", viewName);
    }

    @Test
    public void testRenameTableOnMaterializedViewIfExists()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(viewName, qualifiedName("existing_materialized_view_new"), true)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessageContaining("Table '%s' does not exist, but a materialized view with that name exists. Did you mean ALTER MATERIALIZED VIEW test_catalog.schema.existing_materialized_view RENAME TO ...?", viewName);
    }

    @Test
    public void testRenameTableTargetViewExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(asQualifiedName(tableName), viewName, false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessageContaining("Target table '%s' does not exist, but a view with that name exists.", viewName);
    }

    @Test
    public void testRenameTableTargetMaterializedViewExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, materializedViewName, someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(asQualifiedName(tableName), asQualifiedName(materializedViewName), false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessageContaining("Target table '%s' does not exist, but a materialized view with that name exists.", materializedViewName);
    }

    private ListenableFuture<Void> executeRenameTable(QualifiedName source, QualifiedName target, boolean exists)
    {
        return new RenameTableTask(metadata, new AllowAllAccessControl())
                .execute(new RenameTable(new NodeLocation(1, 1), source, target, exists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
