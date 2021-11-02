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
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
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
        assertThat(metadata.getView(testSession, viewName)).isEmpty();
        assertThat(metadata.getView(testSession, newViewName)).isPresent();
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
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameView(asQualifiedName(tableName), qualifiedName("existing_table_new"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("View '%s' does not exist, but a table with that name exists. Did you mean ALTER TABLE %s RENAME ...?", tableName, tableName);
    }

    @Test
    public void testRenameViewOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameView(viewName, qualifiedName("existing_materialized_view_new"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("View '%s' does not exist, but a materialized view with that name exists. Did you mean ALTER MATERIALIZED VIEW catalog.schema.existing_materialized_view RENAME ...?", viewName);
    }

    private ListenableFuture<Void> executeRenameView(QualifiedName source, QualifiedName target)
    {
        return new RenameViewTask().execute(new RenameView(source, target), transactionManager, metadata, new AllowAllAccessControl(), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
