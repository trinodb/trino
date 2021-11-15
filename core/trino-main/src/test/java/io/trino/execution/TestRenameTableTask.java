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
import io.trino.sql.tree.RenameTable;
import org.testng.annotations.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestRenameTableTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testRenameExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        QualifiedObjectName newTableName = qualifiedObjectName("existing_view_new");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), false);

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
                .hasMessage("Table '%s' does not exist", tableName);
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
        metadata.createView(testSession, viewName, someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(asQualifiedName(viewName), qualifiedName("existing_view_new"), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("Table '%s' does not exist, but a view with that name exists. Did you mean ALTER VIEW %s RENAME ...?", viewName, viewName);
    }

    @Test
    public void testRenameTableOnViewIfExists()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);

        getFutureValue(executeRenameTable(asQualifiedName(viewName), qualifiedName("existing_view_new"), true));
        // no exception
    }

    @Test
    public void testRenameTableOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameTable(viewName, qualifiedName("existing_materialized_view_new"), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("Table '%s' does not exist, but a materialized view with that name exists. Did you mean ALTER MATERIALIZED VIEW catalog.schema.existing_materialized_view RENAME ...?", viewName);
    }

    @Test
    public void testRenameTableOnMaterializedViewIfExists()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), false, false);

        getFutureValue(executeRenameTable(viewName, qualifiedName("existing_materialized_view_new"), true));
        // no exception
    }

    private ListenableFuture<Void> executeRenameTable(QualifiedName source, QualifiedName target, boolean exists)
    {
        return new RenameTableTask().execute(new RenameTable(source, target, exists), transactionManager, metadata, new AllowAllAccessControl(), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
