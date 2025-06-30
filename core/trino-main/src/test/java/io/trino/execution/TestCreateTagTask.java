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
import io.trino.sql.tree.CreateTag;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

public class TestCreateTagTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testCreateTagOnExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(executeCreateTag(asQualifiedName(tableName), "v1", false, false, Optional.empty(), Optional.empty()));
    }

    @Test
    public void testCreateTagWithReplace()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(executeCreateTag(asQualifiedName(tableName), "v1", true, false, Optional.empty(), Optional.empty()));
    }

    @Test
    public void testCreateTagWithIfNotExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(executeCreateTag(asQualifiedName(tableName), "v1", false, true, Optional.empty(), Optional.empty()));
    }

    @Test
    public void testCreateTagWithSnapshotId()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(executeCreateTag(asQualifiedName(tableName), "v1", false, false, Optional.of(123L), Optional.empty()));
    }

    @Test
    public void testCreateTagWithRetentionDays()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(executeCreateTag(asQualifiedName(tableName), "v1", false, false, Optional.empty(), Optional.of(30L)));
    }

    @Test
    public void testCreateTagWithAllOptions()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(executeCreateTag(asQualifiedName(tableName), "v1", true, true, Optional.of(456L), Optional.of(7L)));
    }

    @Test
    public void testCreateTagOnNonExistingTable()
    {
        QualifiedName tableName = qualifiedName("non_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateTag(tableName, "v1", false, false, Optional.empty(), Optional.empty())))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    private ListenableFuture<Void> executeCreateTag(QualifiedName tableName, String tagName, boolean replace, boolean ifNotExists, Optional<Long> snapshotId, Optional<Long> retentionDays)
    {
        return new CreateTagTask(plannerContext, new AllowAllAccessControl()).execute(
                new CreateTag(
                        new NodeLocation(1, 1),
                        tableName,
                        new Identifier(tagName),
                        replace,
                        ifNotExists,
                        snapshotId.map(id -> new LongLiteral(String.valueOf(id))),
                        retentionDays.map(days -> new LongLiteral(String.valueOf(days)))),
                queryStateMachine,
                ImmutableList.of(),
                WarningCollector.NOOP);
    }
}
