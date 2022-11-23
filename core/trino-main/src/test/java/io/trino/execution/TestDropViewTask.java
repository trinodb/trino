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
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestDropViewTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testDropExistingView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);
        assertThat(metadata.isView(testSession, viewName)).isTrue();

        getFutureValue(executeDropView(asQualifiedName(viewName), false));
        assertThat(metadata.isView(testSession, viewName)).isFalse();
    }

    @Test
    public void testDropNotExistingView()
    {
        QualifiedName viewName = qualifiedName("not_existing_view");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropView(viewName, false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("View '%s' does not exist", viewName);
    }

    @Test
    public void testDropNotExistingViewIfExists()
    {
        QualifiedName viewName = qualifiedName("not_existing_view");

        getFutureValue(executeDropView(viewName, true));
        // no exception
    }

    @Test
    public void testDropViewOnTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropView(asQualifiedName(tableName), false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("View '%s' does not exist, but a table with that name exists. Did you mean DROP TABLE %s?", tableName, tableName);
    }

    @Test
    public void testDropViewOnTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropView(asQualifiedName(tableName), true)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("View '%s' does not exist, but a table with that name exists. Did you mean DROP TABLE %s?", tableName, tableName);
    }

    @Test
    public void testDropViewOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropView(viewName, false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("View '%s' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW %s?", viewName, viewName);
    }

    @Test
    public void testDropViewOnMaterializedViewIfExists()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropView(viewName, true)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("View '%s' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW %s?", viewName, viewName);
    }

    private ListenableFuture<Void> executeDropView(QualifiedName viewName, boolean exists)
    {
        return new DropViewTask(metadata, new AllowAllAccessControl()).execute(new DropView(viewName, exists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
