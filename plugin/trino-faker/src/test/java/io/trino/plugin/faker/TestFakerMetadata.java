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
package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

public class TestFakerMetadata
{
    @Test
    public void tableIsCreatedAfterCommits()
    {
        FakerMetadata metadata = createMetadata();
        assertNoTables(metadata);

        SchemaTableName schemaTableName = new SchemaTableName("default", "temp_table");

        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(schemaTableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty(),
                NO_RETRIES,
                false);

        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.empty());
        assertThat(tables.size())
                .describedAs("Expected only one table")
                .isEqualTo(1);
        assertThat(tables.get(0).getTableName())
                .describedAs("Expected table with name 'temp_table'")
                .isEqualTo("temp_table");
    }

    @Test
    public void tableAlreadyExists()
    {
        FakerMetadata metadata = createMetadata();
        assertNoTables(metadata);

        SchemaTableName test1Table = new SchemaTableName("default", "test1");
        SchemaTableName test2Table = new SchemaTableName("default", "test2");
        metadata.createTable(SESSION, new ConnectorTableMetadata(test1Table, ImmutableList.of()), SaveMode.FAIL);

        assertTrinoExceptionThrownBy(() -> metadata.createTable(SESSION, new ConnectorTableMetadata(test1Table, ImmutableList.of()), SaveMode.FAIL))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Table 'default.test1' already exists");

        ConnectorTableHandle test1TableHandle = metadata.getTableHandle(SESSION, test1Table, Optional.empty(), Optional.empty());
        metadata.createTable(SESSION, new ConnectorTableMetadata(test2Table, ImmutableList.of()), SaveMode.FAIL);

        assertTrinoExceptionThrownBy(() -> metadata.renameTable(SESSION, test1TableHandle, test2Table))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Table 'default.test2' already exists");
    }

    @Test
    public void testReadTableBeforeCreationCompleted()
    {
        FakerMetadata metadata = createMetadata();
        assertNoTables(metadata);

        SchemaTableName tableName = new SchemaTableName("default", "temp_table");

        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(tableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty(),
                NO_RETRIES,
                false);

        List<SchemaTableName> tableNames = metadata.listTables(SESSION, Optional.empty());
        assertThat(tableNames.size())
                .describedAs("Expected exactly one table")
                .isEqualTo(1);

        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());
    }

    @Test
    public void testCreateSchema()
    {
        FakerMetadata metadata = createMetadata();
        assertThat(metadata.listSchemaNames(SESSION)).isEqualTo(ImmutableList.of("default"));
        metadata.createSchema(SESSION, "test", ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));
        assertThat(metadata.listSchemaNames(SESSION)).isEqualTo(ImmutableList.of("default", "test"));
        assertThat(metadata.listTables(SESSION, Optional.of("test"))).isEqualTo(ImmutableList.of());

        SchemaTableName tableName = new SchemaTableName("test", "first_table");
        metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        tableName,
                        ImmutableList.of(),
                        ImmutableMap.of()),
                SaveMode.FAIL);

        assertThat(metadata.listTables(SESSION, Optional.empty())).isEqualTo(ImmutableList.of(tableName));
        assertThat(metadata.listTables(SESSION, Optional.of("test"))).isEqualTo(ImmutableList.of(tableName));
        assertThat(metadata.listTables(SESSION, Optional.of("default"))).isEqualTo(ImmutableList.of());
    }

    @Test
    public void testCreateViewWithoutReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");
        FakerMetadata metadata = createMetadata();
        metadata.createSchema(SESSION, "test", ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));
        try {
            metadata.createView(SESSION, test, testingViewDefinition("test"), ImmutableMap.of(), false);
        }
        catch (Exception e) {
            fail("should have succeeded");
        }
        assertThatThrownBy(() -> metadata.createView(SESSION, test, testingViewDefinition("test"), ImmutableMap.of(), false))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("View '%s' already exists".formatted(test));
    }

    @Test
    public void testCreateViewWithReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");

        FakerMetadata metadata = createMetadata();
        metadata.createSchema(SESSION, "test", ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));
        metadata.createView(SESSION, test, testingViewDefinition("aaa"), ImmutableMap.of(), true);
        metadata.createView(SESSION, test, testingViewDefinition("bbb"), ImmutableMap.of(), true);

        assertThat(metadata.getView(SESSION, test))
                .map(ConnectorViewDefinition::getOriginalSql)
                .hasValue("bbb");
    }

    @Test
    public void testCreatedViewShouldBeListedAsTable()
    {
        String schemaName = "test";
        SchemaTableName viewName = new SchemaTableName(schemaName, "test_view");

        FakerMetadata metadata = createMetadata();
        metadata.createSchema(SESSION, schemaName, ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));
        metadata.createView(SESSION, viewName, testingViewDefinition("aaa"), ImmutableMap.of(), true);

        assertThat(metadata.listTables(SESSION, Optional.of(schemaName)))
                .contains(viewName);
    }

    @Test
    public void testViews()
    {
        FakerMetadata metadata = createMetadata();
        SchemaTableName test1 = new SchemaTableName("test", "test_view1");
        SchemaTableName test2 = new SchemaTableName("test", "test_view2");
        SchemaTableName test3 = new SchemaTableName("test", "test_view3");

        // create schema
        metadata.createSchema(SESSION, "test", ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));

        // create views
        metadata.createView(SESSION, test1, testingViewDefinition("test1"), ImmutableMap.of(), false);
        metadata.createView(SESSION, test2, testingViewDefinition("test2"), ImmutableMap.of(), false);

        // verify listing
        List<SchemaTableName> list = metadata.listViews(SESSION, Optional.of("test"));
        assertEqualsIgnoreOrder(list, ImmutableList.of(test1, test2));

        // verify getting data
        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(SESSION, Optional.of("test"));
        assertThat(views.keySet()).isEqualTo(ImmutableSet.of(test1, test2));
        assertThat(views.get(test1).getOriginalSql()).isEqualTo("test1");
        assertThat(views.get(test2).getOriginalSql()).isEqualTo("test2");

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

        // rename second view
        metadata.renameView(SESSION, test2, test3);

        assertThat(metadata.getViews(SESSION, Optional.of("test")))
                .containsOnlyKeys(test3);

        // drop second view
        metadata.dropView(SESSION, test3);

        assertThat(metadata.getViews(SESSION, Optional.of("test")))
                .isEmpty();

        // verify listing everything
        assertThat(metadata.getViews(SESSION, Optional.empty()))
                .isEmpty();
    }

    @Test
    public void testCreateTableAndViewInNotExistSchema()
    {
        FakerMetadata metadata = createMetadata();
        assertThat(metadata.listSchemaNames(SESSION)).isEqualTo(ImmutableList.of("default"));

        SchemaTableName table1 = new SchemaTableName("test1", "test_schema_table1");
        assertTrinoExceptionThrownBy(() -> metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(table1, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty(),
                NO_RETRIES,
                false))
                .hasErrorCode(SCHEMA_NOT_FOUND)
                .hasMessage("Schema 'test1' does not exist");

        SchemaTableName view2 = new SchemaTableName("test2", "test_schema_view2");
        assertTrinoExceptionThrownBy(() -> metadata.createView(SESSION, view2, testingViewDefinition("aaa"), ImmutableMap.of(), false))
                .hasErrorCode(SCHEMA_NOT_FOUND)
                .hasMessage("Schema 'test2' does not exist");

        SchemaTableName view3 = new SchemaTableName("test3", "test_schema_view3");
        assertTrinoExceptionThrownBy(() -> metadata.createView(SESSION, view3, testingViewDefinition("bbb"), ImmutableMap.of(), true))
                .hasErrorCode(SCHEMA_NOT_FOUND)
                .hasMessage("Schema 'test3' does not exist");

        assertThat(metadata.listSchemaNames(SESSION)).isEqualTo(ImmutableList.of("default"));
    }

    @Test
    public void testRenameTable()
    {
        SchemaTableName tableName = new SchemaTableName("test_schema", "test_table_to_be_renamed");
        FakerMetadata metadata = createMetadata();
        metadata.createSchema(SESSION, "test_schema", ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));
        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(tableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty(),
                NO_RETRIES,
                false);
        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());

        // rename table to schema which does not exist
        SchemaTableName invalidSchemaTableName = new SchemaTableName("test_schema_not_exist", "test_table_renamed");
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, tableName, Optional.empty(), Optional.empty());
        assertThat(tableHandle).isNotNull();
        assertTrinoExceptionThrownBy(() -> metadata.renameTable(SESSION, tableHandle, invalidSchemaTableName))
                .isInstanceOf(TrinoException.class)
                .hasErrorCode(SCHEMA_NOT_FOUND)
                .hasMessage("Schema 'test_schema_not_exist' does not exist");

        // rename table to same schema
        ConnectorTableHandle originalTableHandle = metadata.getTableHandle(SESSION, tableName, Optional.empty(), Optional.empty());
        assertThat(originalTableHandle).isNotNull();
        SchemaTableName sameSchemaTableName = new SchemaTableName("test_schema", "test_renamed");
        metadata.renameTable(SESSION, originalTableHandle, sameSchemaTableName);
        assertThat(metadata.listTables(SESSION, Optional.of("test_schema"))).isEqualTo(ImmutableList.of(sameSchemaTableName));

        // rename table to different schema
        metadata.createSchema(SESSION, "test_different_schema", ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));
        ConnectorTableHandle renamedTableHandle = metadata.getTableHandle(SESSION, sameSchemaTableName, Optional.empty(), Optional.empty());
        assertThat(renamedTableHandle).isNotNull();
        SchemaTableName differentSchemaTableName = new SchemaTableName("test_different_schema", "test_renamed");
        metadata.renameTable(SESSION, renamedTableHandle, differentSchemaTableName);
        assertThat(metadata.listTables(SESSION, Optional.of("test_schema"))).isEqualTo(ImmutableList.of());
        assertThat(metadata.listTables(SESSION, Optional.of("test_different_schema"))).isEqualTo(ImmutableList.of(differentSchemaTableName));
    }

    private static void assertNoTables(FakerMetadata metadata)
    {
        assertThat(metadata.listTables(SESSION, Optional.empty()))
                .describedAs("No table was expected")
                .isEqualTo(ImmutableList.of());
    }

    private static ConnectorViewDefinition testingViewDefinition(String sql)
    {
        return new ConnectorViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ViewColumn("test", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                true,
                ImmutableList.of());
    }

    private static FakerMetadata createMetadata()
    {
        return new FakerMetadata(new FakerConfig(), new FakerFunctionProvider(), new CatalogName("test"));
    }
}
