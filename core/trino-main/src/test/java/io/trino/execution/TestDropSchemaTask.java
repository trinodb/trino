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
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestDropSchemaTask
        extends BaseDataDefinitionTaskTest
{
    private static final CatalogSchemaName CATALOG_SCHEMA_NAME = new CatalogSchemaName(TEST_CATALOG_NAME, "test_db");

    @Test
    public void testDropSchemaRestrict()
    {
        CreateSchemaTask createSchemaTask = getCreateSchemaTask();
        CreateSchema createSchema = new CreateSchema(new NodeLocation(1, 1), QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, ImmutableList.of(), Optional.empty());
        getFutureValue(createSchemaTask.execute(createSchema, queryStateMachine, emptyList(), NOOP));
        assertThat(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME)).isTrue();

        DropSchemaTask dropSchemaTask = getDropSchemaTask();
        DropSchema dropSchema = new DropSchema(new NodeLocation(1, 1), QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, false);
        getFutureValue(dropSchemaTask.execute(dropSchema, queryStateMachine, emptyList(), NOOP));
        assertThat(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME)).isFalse();

        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(dropSchemaTask.execute(dropSchema, queryStateMachine, emptyList(), NOOP)))
                .withMessageContaining("Schema 'test_catalog.test_db' does not exist");
    }

    @Test
    public void testDropNonEmptySchemaRestrict()
    {
        CreateSchemaTask createSchemaTask = getCreateSchemaTask();
        CreateSchema createSchema = new CreateSchema(new NodeLocation(1, 1), QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, ImmutableList.of(), Optional.empty());
        getFutureValue(createSchemaTask.execute(createSchema, queryStateMachine, emptyList(), NOOP));

        DropSchemaTask dropSchemaTask = getDropSchemaTask();
        DropSchema dropSchema = new DropSchema(new NodeLocation(1, 1), QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, false);

        QualifiedObjectName tableName = new QualifiedObjectName(CATALOG_SCHEMA_NAME.getCatalogName(), CATALOG_SCHEMA_NAME.getSchemaName(), "test_table");
        metadata.createTable(testSession, CATALOG_SCHEMA_NAME.getCatalogName(), someTable(tableName), FAIL);

        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(dropSchemaTask.execute(dropSchema, queryStateMachine, emptyList(), NOOP)))
                .withMessageContaining("Cannot drop non-empty schema 'test_db'");
        assertThat(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME)).isTrue();
    }

    @Test
    public void testDropSchemaIfExistsRestrict()
    {
        CatalogSchemaName schema = new CatalogSchemaName(CATALOG_SCHEMA_NAME.getCatalogName(), "test_if_exists_restrict");

        assertThat(metadata.schemaExists(testSession, schema)).isFalse();
        DropSchemaTask dropSchemaTask = getDropSchemaTask();

        DropSchema dropSchema = new DropSchema(new NodeLocation(1, 1), QualifiedName.of("test_if_exists_restrict"), true, false);
        getFutureValue(dropSchemaTask.execute(dropSchema, queryStateMachine, emptyList(), NOOP));
    }

    @Test
    public void testDropSchemaCascade()
    {
        CreateSchemaTask createSchemaTask = getCreateSchemaTask();
        CreateSchema createSchema = new CreateSchema(new NodeLocation(1, 1), QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, ImmutableList.of(), Optional.empty());
        getFutureValue(createSchemaTask.execute(createSchema, queryStateMachine, emptyList(), NOOP));
        assertThat(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME)).isTrue();

        DropSchemaTask dropSchemaTask = getDropSchemaTask();
        DropSchema dropSchema = new DropSchema(new NodeLocation(1, 1), QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, true);

        getFutureValue(dropSchemaTask.execute(dropSchema, queryStateMachine, emptyList(), NOOP));
        assertThat(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME)).isFalse();
    }

    @Test
    public void testDropNonEmptySchemaCascade()
    {
        CreateSchemaTask createSchemaTask = getCreateSchemaTask();
        CreateSchema createSchema = new CreateSchema(new NodeLocation(1, 1), QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, ImmutableList.of(), Optional.empty());
        getFutureValue(createSchemaTask.execute(createSchema, queryStateMachine, emptyList(), NOOP));

        DropSchemaTask dropSchemaTask = getDropSchemaTask();
        DropSchema dropSchema = new DropSchema(new NodeLocation(1, 1), QualifiedName.of(CATALOG_SCHEMA_NAME.getSchemaName()), false, true);

        QualifiedObjectName tableName = new QualifiedObjectName(CATALOG_SCHEMA_NAME.getCatalogName(), CATALOG_SCHEMA_NAME.getSchemaName(), "test_table");
        metadata.createTable(testSession, CATALOG_SCHEMA_NAME.getCatalogName(), someTable(tableName), FAIL);

        getFutureValue(dropSchemaTask.execute(dropSchema, queryStateMachine, emptyList(), NOOP));
        assertThat(metadata.schemaExists(testSession, CATALOG_SCHEMA_NAME)).isFalse();
    }

    @Test
    public void testDropSchemaIfExistsCascade()
    {
        CatalogSchemaName schema = new CatalogSchemaName(CATALOG_SCHEMA_NAME.getCatalogName(), "test_if_exists_cascade");

        assertThat(metadata.schemaExists(testSession, schema)).isFalse();
        DropSchemaTask dropSchemaTask = getDropSchemaTask();

        DropSchema dropSchema = new DropSchema(new NodeLocation(1, 1), QualifiedName.of("test_if_exists_cascade"), true, false);
        getFutureValue(dropSchemaTask.execute(dropSchema, queryStateMachine, emptyList(), NOOP));
    }

    private CreateSchemaTask getCreateSchemaTask()
    {
        SchemaPropertyManager schemaPropertyManager = new SchemaPropertyManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, ImmutableMap.of()));
        return new CreateSchemaTask(plannerContext, new AllowAllAccessControl(), schemaPropertyManager);
    }

    private DropSchemaTask getDropSchemaTask()
    {
        return new DropSchemaTask(metadata, new AllowAllAccessControl());
    }
}
