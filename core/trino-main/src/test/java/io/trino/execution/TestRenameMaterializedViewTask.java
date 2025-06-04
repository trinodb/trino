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
import io.trino.sql.tree.RenameMaterializedView;
import org.junit.jupiter.api.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRenameMaterializedViewTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testRenameExistingMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        QualifiedObjectName newMaterializedViewName = qualifiedObjectName("existing_materialized_view_new");
        metadata.createMaterializedView(testSession, materializedViewName, someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        getFutureValue(executeRenameMaterializedView(asQualifiedName(materializedViewName), asQualifiedName(newMaterializedViewName)));
        assertThat(metadata.isMaterializedView(testSession, materializedViewName)).isFalse();
        assertThat(metadata.isMaterializedView(testSession, newMaterializedViewName)).isTrue();
    }

    @Test
    public void testRenameNotExistingMaterializedView()
    {
        QualifiedName materializedViewName = qualifiedName("not_existing_materialized_view");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameMaterializedView(materializedViewName, qualifiedName("not_existing_materialized_view_new"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Materialized View '%s' does not exist", materializedViewName);
    }

    @Test
    public void testRenameNotExistingMaterializedViewIfExists()
    {
        QualifiedName materializedViewName = qualifiedName("not_existing_materialized_view");

        getFutureValue(executeRenameMaterializedView(materializedViewName, qualifiedName("not_existing_materialized_view_new"), true));
        // no exception
    }

    @Test
    public void testRenameMaterializedViewOnTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameMaterializedView(asQualifiedName(tableName), qualifiedName("existing_table_new"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Materialized View '%s' does not exist, but a table with that name exists. Did you mean ALTER TABLE %s RENAME TO ...?", tableName, tableName);
    }

    @Test
    public void testRenameMaterializedViewOnTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameMaterializedView(asQualifiedName(tableName), qualifiedName("existing_table_new"), true)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Materialized View '%s' does not exist, but a table with that name exists. Did you mean ALTER TABLE %s RENAME TO ...?", tableName, tableName);
    }

    @Test
    public void testRenameMaterializedViewTargetTableExists()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, materializedViewName, someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameMaterializedView(asQualifiedName(materializedViewName), asQualifiedName(tableName))))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessageContaining("Target materialized view '%s' does not exist, but a table with that name exists.", tableName);
    }

    @Test
    public void testRenameMaterializedViewOnView()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameMaterializedView(viewName, qualifiedName("existing_view_new"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Materialized View '%s' does not exist, but a view with that name exists. Did you mean ALTER VIEW test_catalog.schema.existing_view RENAME TO ...?", viewName);
    }

    @Test
    public void testRenameMaterializedViewOnViewIfExists()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameMaterializedView(viewName, qualifiedName("existing_view_new"), true)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Materialized View '%s' does not exist, but a view with that name exists. Did you mean ALTER VIEW test_catalog.schema.existing_view RENAME TO ...?", viewName);
    }

    @Test
    public void testRenameMaterializedViewTargetViewExists()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, materializedViewName, someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameMaterializedView(asQualifiedName(materializedViewName), viewName)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessageContaining("Target materialized view '%s' does not exist, but a view with that name exists.", viewName);
    }

    private ListenableFuture<Void> executeRenameMaterializedView(QualifiedName source, QualifiedName target)
    {
        return executeRenameMaterializedView(source, target, false);
    }

    private ListenableFuture<Void> executeRenameMaterializedView(QualifiedName source, QualifiedName target, boolean exists)
    {
        return new RenameMaterializedViewTask(metadata, new AllowAllAccessControl())
                .execute(new RenameMaterializedView(new NodeLocation(1, 1), source, target, exists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
