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
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
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
        assertThat(metadata.isMaterializedView(testSession, viewName)).isTrue();

        getFutureValue(executeDropMaterializedView(asQualifiedName(viewName), false));
        assertThat(metadata.isMaterializedView(testSession, viewName)).isFalse();
    }

    @Test
    public void testDropNotExistingMaterializedView()
    {
        QualifiedName viewName = qualifiedName("not_existing_materialized_view");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropMaterializedView(viewName, false)))
                .hasErrorCode(GENERIC_USER_ERROR)
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
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropMaterializedView(asQualifiedName(tableName), true)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Materialized view '%s' does not exist, but a table with that name exists. Did you mean DROP TABLE %s?", tableName, tableName);
    }

    @Test
    public void testDropMaterializedViewOnTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropMaterializedView(asQualifiedName(tableName), true)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Materialized view '%s' does not exist, but a table with that name exists. Did you mean DROP TABLE %s?", tableName, tableName);
    }

    @Test
    public void testDropMaterializedViewOnView()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, asQualifiedObjectName(viewName), someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropMaterializedView(viewName, false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Materialized view '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", viewName, viewName);
    }

    @Test
    public void testDropMaterializedViewOnViewIfExists()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, asQualifiedObjectName(viewName), someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropMaterializedView(viewName, false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Materialized view '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", viewName, viewName);
    }

    private ListenableFuture<Void> executeDropMaterializedView(QualifiedName viewName, boolean exists)
    {
        return new DropMaterializedViewTask(metadata, new AllowAllAccessControl())
                .execute(new DropMaterializedView(viewName, exists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
