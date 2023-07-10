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
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestDropTableTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testDropExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), false);
        assertThat(metadata.getTableHandle(testSession, tableName)).isPresent();

        getFutureValue(executeDropTable(asQualifiedName(tableName), false));
        assertThat(metadata.getTableHandle(testSession, tableName)).isEmpty();
    }

    @Test
    public void testDropNotExistingTable()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropTable(tableName, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("Table '%s' does not exist", tableName);
    }

    @Test
    public void testDropTableIfExistsWithoutExistingCatalog()
    {
        QualifiedName tableName = QualifiedName.of("non_existing_catalog", "non_existing_schema", "not_existing_table");

        getFutureValue(executeDropTable(tableName, true));
        // no exception
    }

    @Test
    public void testDropTableIfExistsWithoutExistingSchema()
    {
        QualifiedName tableName = QualifiedName.of(TEST_CATALOG_NAME, "non_existing_schema", "not_existing_table");

        getFutureValue(executeDropTable(tableName, true));
        // no exception
    }

    @Test
    public void testDropTableIfExistsWithoutExistingTable()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeDropTable(tableName, true));
        // no exception
    }

    @Test
    public void testDropTableOnView()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, asQualifiedObjectName(viewName), someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropTable(viewName, false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Table '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", viewName, viewName);
    }

    @Test
    public void testDropTableIfExistsOnView()
    {
        QualifiedName viewName = qualifiedName("existing_view");
        metadata.createView(testSession, asQualifiedObjectName(viewName), someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropTable(viewName, true)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Table '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", viewName, viewName);
    }

    @Test
    public void testDropTableOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, asQualifiedObjectName(viewName), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropTable(viewName, false)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Table '%s' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW %s?", viewName, viewName);
    }

    @Test
    public void testDropTableIfExistsOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, asQualifiedObjectName(viewName), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropTable(viewName, true)))
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("Table '%s' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW %s?", viewName, viewName);
    }

    private ListenableFuture<Void> executeDropTable(QualifiedName tableName, boolean exists)
    {
        return new DropTableTask(metadata, new AllowAllAccessControl()).execute(new DropTable(tableName, exists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
