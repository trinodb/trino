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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.TestingNodeManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestMemoryMetadata
{
    private MemoryMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        metadata = new MemoryMetadata(new TestingNodeManager());
    }

    @Test
    public void tableIsCreatedAfterCommits()
    {
        assertNoTables();

        SchemaTableName schemaTableName = new SchemaTableName("default", "temp_table");

        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(schemaTableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty());

        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.empty());
        assertTrue(tables.size() == 1, "Expected only one table");
        assertTrue(tables.get(0).getTableName().equals("temp_table"), "Expected table with name 'temp_table'");
    }

    @Test
    public void tableAlreadyExists()
    {
        assertNoTables();

        SchemaTableName test1Table = new SchemaTableName("default", "test1");
        SchemaTableName test2Table = new SchemaTableName("default", "test2");
        metadata.createTable(SESSION, new ConnectorTableMetadata(test1Table, ImmutableList.of()), false);

        try {
            metadata.createTable(SESSION, new ConnectorTableMetadata(test1Table, ImmutableList.of()), false);
            fail("Should fail because table already exists");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), ALREADY_EXISTS.toErrorCode());
            assertEquals(ex.getMessage(), "Table [default.test1] already exists");
        }

        ConnectorTableHandle test1TableHandle = metadata.getTableHandle(SESSION, test1Table);
        metadata.createTable(SESSION, new ConnectorTableMetadata(test2Table, ImmutableList.of()), false);

        try {
            metadata.renameTable(SESSION, test1TableHandle, test2Table);
            fail("Should fail because table already exists");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), ALREADY_EXISTS.toErrorCode());
            assertEquals(ex.getMessage(), "Table [default.test2] already exists");
        }
    }

    @Test
    public void testActiveTableIds()
    {
        assertNoTables();

        SchemaTableName firstTableName = new SchemaTableName("default", "first_table");
        metadata.createTable(SESSION, new ConnectorTableMetadata(firstTableName, ImmutableList.of(), ImmutableMap.of()), false);

        MemoryTableHandle firstTableHandle = (MemoryTableHandle) metadata.getTableHandle(SESSION, firstTableName);
        long firstTableId = firstTableHandle.getId();

        assertTrue(metadata.beginInsert(SESSION, firstTableHandle).getActiveTableIds().contains(firstTableId));

        SchemaTableName secondTableName = new SchemaTableName("default", "second_table");
        metadata.createTable(SESSION, new ConnectorTableMetadata(secondTableName, ImmutableList.of(), ImmutableMap.of()), false);

        MemoryTableHandle secondTableHandle = (MemoryTableHandle) metadata.getTableHandle(SESSION, secondTableName);
        long secondTableId = secondTableHandle.getId();

        assertNotEquals(firstTableId, secondTableId);
        assertTrue(metadata.beginInsert(SESSION, secondTableHandle).getActiveTableIds().contains(firstTableId));
        assertTrue(metadata.beginInsert(SESSION, secondTableHandle).getActiveTableIds().contains(secondTableId));
    }

    @Test
    public void testReadTableBeforeCreationCompleted()
    {
        assertNoTables();

        SchemaTableName tableName = new SchemaTableName("default", "temp_table");

        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(tableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty());

        List<SchemaTableName> tableNames = metadata.listTables(SESSION, Optional.empty());
        assertTrue(tableNames.size() == 1, "Expected exactly one table");

        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());
    }

    @Test
    public void testCreateSchema()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default"));
        metadata.createSchema(SESSION, "test", ImmutableMap.of());
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default", "test"));
        assertEquals(metadata.listTables(SESSION, Optional.of("test")), ImmutableList.of());

        SchemaTableName tableName = new SchemaTableName("test", "first_table");
        metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        tableName,
                        ImmutableList.of(),
                        ImmutableMap.of()),
                false);

        assertEquals(metadata.listTables(SESSION, Optional.empty()), ImmutableList.of(tableName));
        assertEquals(metadata.listTables(SESSION, Optional.of("test")), ImmutableList.of(tableName));
        assertEquals(metadata.listTables(SESSION, Optional.of("default")), ImmutableList.of());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "View already exists: test\\.test_view")
    public void testCreateViewWithoutReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");
        metadata.createSchema(SESSION, "test", ImmutableMap.of());
        try {
            metadata.createView(SESSION, test, testingViewDefinition("test"), false);
        }
        catch (Exception e) {
            fail("should have succeeded");
        }
        metadata.createView(SESSION, test, testingViewDefinition("test"), false);
    }

    @Test
    public void testCreateViewWithReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");

        metadata.createSchema(SESSION, "test", ImmutableMap.of());
        metadata.createView(SESSION, test, testingViewDefinition("aaa"), true);
        metadata.createView(SESSION, test, testingViewDefinition("bbb"), true);

        assertThat(metadata.getView(SESSION, test))
                .map(ConnectorViewDefinition::getOriginalSql)
                .hasValue("bbb");
    }

    @Test
    public void testViews()
    {
        SchemaTableName test1 = new SchemaTableName("test", "test_view1");
        SchemaTableName test2 = new SchemaTableName("test", "test_view2");

        // create schema
        metadata.createSchema(SESSION, "test", ImmutableMap.of());

        // create views
        metadata.createView(SESSION, test1, testingViewDefinition("test1"), false);
        metadata.createView(SESSION, test2, testingViewDefinition("test2"), false);

        // verify listing
        List<SchemaTableName> list = metadata.listViews(SESSION, Optional.of("test"));
        assertEqualsIgnoreOrder(list, ImmutableList.of(test1, test2));

        // verify getting data
        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(SESSION, Optional.of("test"));
        assertEquals(views.keySet(), ImmutableSet.of(test1, test2));
        assertEquals(views.get(test1).getOriginalSql(), "test1");
        assertEquals(views.get(test2).getOriginalSql(), "test2");

        // all schemas
        assertThat(metadata.getViews(SESSION, Optional.empty()))
                .containsOnlyKeys(test1, test2);

        // exact match on one schema and table
        assertThat(metadata.getView(SESSION, new SchemaTableName("test", "test_view1")))
                .map(ConnectorViewDefinition::getOriginalSql)
                .contains("test1");

        // non-existent table
        assertThat(metadata.getView(SESSION, new SchemaTableName("test", "nonexistenttable")))
                .isEmpty();

        // non-existent schema
        assertThat(metadata.getViews(SESSION, Optional.of("nonexistentschema")))
                .isEmpty();

        // drop first view
        metadata.dropView(SESSION, test1);

        assertThat(metadata.getViews(SESSION, Optional.of("test")))
                .containsOnlyKeys(test2);

        // drop second view
        metadata.dropView(SESSION, test2);

        assertThat(metadata.getViews(SESSION, Optional.of("test")))
                .isEmpty();

        // verify listing everything
        assertThat(metadata.getViews(SESSION, Optional.empty()))
                .isEmpty();
    }

    @Test
    public void testCreateTableAndViewInNotExistSchema()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default"));

        SchemaTableName table1 = new SchemaTableName("test1", "test_schema_table1");
        try {
            metadata.beginCreateTable(SESSION, new ConnectorTableMetadata(table1, ImmutableList.of(), ImmutableMap.of()), Optional.empty());
            fail("Should fail because schema does not exist");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(ex.getMessage(), "Schema test1 not found");
        }
        assertEquals(metadata.getTableHandle(SESSION, table1), null);

        SchemaTableName view2 = new SchemaTableName("test2", "test_schema_view2");
        try {
            metadata.createView(SESSION, view2, testingViewDefinition("aaa"), false);
            fail("Should fail because schema does not exist");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(ex.getMessage(), "Schema test2 not found");
        }
        assertEquals(metadata.getTableHandle(SESSION, view2), null);

        SchemaTableName view3 = new SchemaTableName("test3", "test_schema_view3");
        try {
            metadata.createView(SESSION, view3, testingViewDefinition("bbb"), true);
            fail("Should fail because schema does not exist");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(ex.getMessage(), "Schema test3 not found");
        }
        assertEquals(metadata.getTableHandle(SESSION, view3), null);

        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default"));
    }

    @Test
    public void testRenameTable()
    {
        SchemaTableName tableName = new SchemaTableName("test_schema", "test_table_to_be_renamed");
        metadata.createSchema(SESSION, "test_schema", ImmutableMap.of());
        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(tableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty());
        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());

        // rename table to schema which does not exist
        SchemaTableName invalidSchemaTableName = new SchemaTableName("test_schema_not_exist", "test_table_renamed");
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, tableName);
        Throwable throwable = expectThrows(SchemaNotFoundException.class, () -> metadata.renameTable(SESSION, tableHandle, invalidSchemaTableName));
        assertTrue(throwable.getMessage().equals("Schema test_schema_not_exist not found"));

        // rename table to same schema
        SchemaTableName sameSchemaTableName = new SchemaTableName("test_schema", "test_renamed");
        metadata.renameTable(SESSION, metadata.getTableHandle(SESSION, tableName), sameSchemaTableName);
        assertEquals(metadata.listTables(SESSION, Optional.of("test_schema")), ImmutableList.of(sameSchemaTableName));

        // rename table to different schema
        metadata.createSchema(SESSION, "test_different_schema", ImmutableMap.of());
        SchemaTableName differentSchemaTableName = new SchemaTableName("test_different_schema", "test_renamed");
        metadata.renameTable(SESSION, metadata.getTableHandle(SESSION, sameSchemaTableName), differentSchemaTableName);
        assertEquals(metadata.listTables(SESSION, Optional.of("test_schema")), ImmutableList.of());
        assertEquals(metadata.listTables(SESSION, Optional.of("test_different_schema")), ImmutableList.of(differentSchemaTableName));
    }

    private void assertNoTables()
    {
        assertEquals(metadata.listTables(SESSION, Optional.empty()), ImmutableList.of(), "No table was expected");
    }

    private static ConnectorViewDefinition testingViewDefinition(String sql)
    {
        return new ConnectorViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ViewColumn("test", BIGINT.getTypeSignature())),
                Optional.empty(),
                true);
    }
}
