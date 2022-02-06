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
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.RenameView;
import org.testng.annotations.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestRenameViewTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testRenameExistingView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        QualifiedObjectName newViewName = qualifiedObjectName("existing_view_new");
        metadata.createView(testSession, viewName, someView(), false);

        getFutureValue(executeRenameView(asQualifiedName(viewName), asQualifiedName(newViewName)));
        assertThat(metadata.isView(testSession, viewName)).isFalse();
        assertThat(metadata.isView(testSession, newViewName)).isTrue();
    }

    @Test
    public void testRenameNotExistingView()
    {
        QualifiedName viewName = qualifiedName("not_existing_view");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameView(viewName, qualifiedName("not_existing_view_new"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("View '%s' does not exist", viewName);
    }

    @Test
    public void testRenameViewOnTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameView(asQualifiedName(tableName), qualifiedName("existing_table_new"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("View '%s' does not exist, but a table with that name exists. Did you mean ALTER TABLE %s RENAME TO ...?", tableName, tableName);
    }

    @Test
    public void testRenameViewOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameView(viewName, qualifiedName("existing_materialized_view_new"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("View '%s' does not exist, but a materialized view with that name exists. Did you mean ALTER MATERIALIZED VIEW test-catalog.schema.existing_materialized_view RENAME TO ...?", viewName);
    }

    @Test
    public void testRenameViewTargetTableExists()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someView(), false);
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameView(viewName, asQualifiedName(tableName))))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Target view '%s' does not exist, but a table with that name exists.", tableName);
    }

    @Test
    public void testRenameViewTargetMaterializedViewExists()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someView(), false);
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, materializedViewName, someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameView(viewName, asQualifiedName(materializedViewName))))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Target view '%s' does not exist, but a materialized view with that name exists.", materializedViewName);
    }

    @Test
    public void testRenameViewTargetViewExists()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameView(viewName, viewName)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Target view '%s' already exists", viewName);
    }

    private ListenableFuture<Void> executeRenameView(QualifiedName source, QualifiedName target)
    {
        return new RenameViewTask(metadata, new AllowAllAccessControl())
                .execute(new RenameView(source, target), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
