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
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCreateSchemaTask
        extends BaseDataDefinitionTaskTest
{
    private static final CatalogSchemaName CATALOG_SCHEMA_NAME = new CatalogSchemaName(TEST_CATALOG_NAME, "test_db");

    @Test
    public void testDuplicatedCreateSchema()
    {
        CreateSchemaTask task = getCreateSchemaTask();
        CreateSchema statement = new CreateSchema(QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, ImmutableList.of());
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertTrue(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME));
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP)))
                .withMessage("Schema 'test-catalog.test_db' already exists");
    }

    @Test
    public void testDuplicatedCreateSchemaIfNotExists()
    {
        CreateSchemaTask task = getCreateSchemaTask();
        CreateSchema statement = new CreateSchema(QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), true, ImmutableList.of());
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertTrue(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME));
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertTrue(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME));
    }

    @Test
    public void failCreateSchema()
    {
        CreateSchemaTask task = getCreateSchemaTask();
        metadata.failCreateSchema();
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(task.execute(
                        new CreateSchema(QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, ImmutableList.of()),
                        queryStateMachine,
                        emptyList(),
                        WarningCollector.NOOP)))
                .withMessage("TEST create schema fail: test-catalog.test_db");
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(task.execute(
                        new CreateSchema(QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), true, ImmutableList.of()),
                        queryStateMachine,
                        emptyList(),
                        WarningCollector.NOOP)))
                .withMessage("TEST create schema fail: test-catalog.test_db");
    }

    private CreateSchemaTask getCreateSchemaTask()
    {
        SchemaPropertyManager schemaPropertyManager = new SchemaPropertyManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, ImmutableMap.of()));
        return new CreateSchemaTask(plannerContext, new AllowAllAccessControl(), schemaPropertyManager);
    }
}
