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
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestDropMaterializedViewTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testDropExistingMaterializedView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, viewName, someMaterializedView(), false, false);
        assertThat(metadata.getMaterializedView(testSession, viewName)).isPresent();

        getFutureValue(executeDropMaterializedView(asQualifiedName(viewName), false));
        assertThat(metadata.getMaterializedView(testSession, viewName)).isEmpty();
    }

    @Test
    public void testDropNotExistingMaterializedView()
    {
        QualifiedName viewName = qualifiedName("not_existing_materialized_view");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropMaterializedView(viewName, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("Materialized view '%s' does not exist", viewName);
    }

    @Test
    public void testDropNotExistingMaterializedViewIfExists()
    {
        QualifiedName viewName = qualifiedName("not_existing_materialized_view");

        getFutureValue(executeDropMaterializedView(viewName, true));
        // no exception
    }

    @Test
    public void testDropMaterializedViewOnTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropMaterializedView(asQualifiedName(tableName), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("Materialized view '%s' does not exist, but a table with that name exists. Did you mean DROP TABLE %s?", tableName, tableName);
    }

    @Test
    public void testDropMaterializedViewOnTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), false);

        getFutureValue(executeDropMaterializedView(asQualifiedName(tableName), true));
        // no exception
    }

    @Test
    public void testDropMaterializedViewOnView()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, asQualifiedObjectName(viewName), someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropMaterializedView(viewName, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("Materialized view '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", viewName, viewName);
    }

    @Test
    public void testDropMaterializedViewOnViewIfExists()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, asQualifiedObjectName(viewName), someView(), false);

        getFutureValue(executeDropMaterializedView(viewName, true));
        // no exception
    }

    private ListenableFuture<Void> executeDropMaterializedView(QualifiedName viewName, boolean exists)
    {
        return new DropMaterializedViewTask().execute(new DropMaterializedView(viewName, exists), transactionManager, metadata, new AllowAllAccessControl(), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
