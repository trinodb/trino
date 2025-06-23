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
import io.trino.sql.tree.DropTag;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

public class TestDropTagTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testDropTagOnExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(executeDropTag(asQualifiedName(tableName), "v1"));
    }

    @Test
    public void testDropTagOnNonExistingTable()
    {
        QualifiedName tableName = qualifiedName("non_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropTag(tableName, "v1")))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    private ListenableFuture<Void> executeDropTag(QualifiedName tableName, String tagName)
    {
        return new DropTagTask(plannerContext, new AllowAllAccessControl()).execute(
                new DropTag(
                        new NodeLocation(1, 1),
                        tableName,
                        new Identifier(tagName)),
                queryStateMachine,
                ImmutableList.of(),
                WarningCollector.NOOP);
    }
}
