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
package io.trino.plugin.blackhole;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.connector.SaveMode.IGNORE;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Map.entry;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestBlackHoleMetadata
{
    private final BlackHoleMetadata metadata = new BlackHoleMetadata();
    private final Map<String, Object> tableProperties = ImmutableMap.of(
            BlackHoleConnector.SPLIT_COUNT_PROPERTY, 0,
            BlackHoleConnector.PAGES_PER_SPLIT_PROPERTY, 0,
            BlackHoleConnector.ROWS_PER_PAGE_PROPERTY, 0,
            BlackHoleConnector.FIELD_LENGTH_PROPERTY, 16,
            BlackHoleConnector.PAGE_PROCESSING_DELAY, new Duration(0, SECONDS));

    @Test
    void testCreateSchema()
    {
        assertThat(metadata.listSchemaNames(SESSION)).isEqualTo(ImmutableList.of("default"));
        metadata.createSchema(SESSION, "test", ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));
        assertThat(metadata.listSchemaNames(SESSION)).isEqualTo(ImmutableList.of("default", "test"));
    }

    @Test
    void tableIsCreatedAfterCommits()
    {
        assertThatNoTableIsCreated();

        SchemaTableName schemaTableName = new SchemaTableName("default", "temp_table");

        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(schemaTableName, ImmutableList.of(), tableProperties),
                Optional.empty(),
                NO_RETRIES,
                false);

        assertThatNoTableIsCreated();

        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.empty());
        assertThat(tables).hasSize(1);
        assertThat(tables.get(0).getTableName()).isEqualTo("temp_table");
    }

    @Test
    void testCreateTableInNotExistSchema()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema1", "test_table");
        assertTrinoExceptionThrownBy(() -> metadata.beginCreateTable(SESSION, new ConnectorTableMetadata(schemaTableName, ImmutableList.of(), tableProperties), Optional.empty(), NO_RETRIES, false))
                .hasErrorCode(NOT_FOUND)
                .hasMessage("Schema schema1 not found");
    }

    @Test
    void testGetView()
    {
        metadata.createSchema(SESSION, "default2", ImmutableMap.of(), new TrinoPrincipal(USER, SESSION.getUser()));
        ConnectorViewDefinition viewDefinition = createViewDefinition();
        Map<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.<SchemaTableName, ConnectorViewDefinition>builder()
                .put(new SchemaTableName("default", "test_view"), viewDefinition)
                .put(new SchemaTableName("default", "test_view2"), viewDefinition)
                .put(new SchemaTableName("default2", "test_view2"), viewDefinition)
                .buildOrThrow();

        for (Entry<SchemaTableName, ConnectorViewDefinition> entry : views.entrySet()) {
            metadata.createView(SESSION, entry.getKey(), entry.getValue(), ImmutableMap.of(), FAIL);
        }

        assertThat(metadata.listViews(SESSION, Optional.empty()))
                .containsExactlyElementsOf(views.keySet());
        assertThat(metadata.listViews(SESSION, Optional.of("default")))
                .containsExactly(new SchemaTableName("default", "test_view"), new SchemaTableName("default", "test_view2"));

        assertThat(metadata.getViews(SESSION, Optional.empty()))
                .containsExactlyEntriesOf(views);
        assertThat(metadata.getViews(SESSION, Optional.of("default2")))
                .containsExactly(entry(new SchemaTableName("default2", "test_view2"), viewDefinition));
    }

    @Test
    void testCreateViewSaveModes()
    {
        SchemaTableName viewName = new SchemaTableName("default", "test_view");
        ConnectorViewDefinition original = createViewDefinition();
        ConnectorViewDefinition other = new ConnectorViewDefinition(
                "SELECT 2",
                Optional.of("blackhole"),
                Optional.of("default"),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("col2", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                true,
                ImmutableList.of());

        metadata.createView(SESSION, viewName, original, ImmutableMap.of(), FAIL);

        // IGNORE keeps the existing view
        metadata.createView(SESSION, viewName, other, ImmutableMap.of(), IGNORE);
        assertThat(metadata.getView(SESSION, viewName)).contains(original);

        // FAIL rejects an existing view
        assertTrinoExceptionThrownBy(() -> metadata.createView(SESSION, viewName, other, ImmutableMap.of(), FAIL))
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("View 'default.test_view' already exists");
        assertThat(metadata.getView(SESSION, viewName)).contains(original);

        // REPLACE overwrites it
        metadata.createView(SESSION, viewName, other, ImmutableMap.of(), REPLACE);
        assertThat(metadata.getView(SESSION, viewName)).contains(other);
    }

    @Test
    void testCreateViewInNotExistSchema()
    {
        SchemaTableName viewName = new SchemaTableName("schema1", "test_view");
        assertTrinoExceptionThrownBy(() -> metadata.createView(SESSION, viewName, createViewDefinition(), ImmutableMap.of(), FAIL))
                .hasErrorCode(NOT_FOUND)
                .hasMessage("Schema schema1 not found");
    }

    @Test
    void testCreateViewOnExistingTable()
    {
        SchemaTableName tableName = new SchemaTableName("default", "test_table");
        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(tableName, ImmutableList.of(), tableProperties),
                Optional.empty(),
                NO_RETRIES,
                false);
        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());

        for (SaveMode saveMode : SaveMode.values()) {
            assertTrinoExceptionThrownBy(() -> metadata.createView(SESSION, tableName, createViewDefinition(), ImmutableMap.of(), saveMode))
                    .hasErrorCode(TABLE_ALREADY_EXISTS)
                    .hasMessage("Table 'default.test_table' already exists");
        }
        assertThat(metadata.getView(SESSION, tableName)).isEmpty();
    }

    private static ConnectorViewDefinition createViewDefinition()
    {
        return new ConnectorViewDefinition(
                "SELECT * FROM test_table",
                Optional.of("blackhole"),
                Optional.of("default"),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("col1", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                true,
                ImmutableList.of());
    }

    private void assertThatNoTableIsCreated()
    {
        assertThat(metadata.listTables(SESSION, Optional.empty())).isEmpty();
    }
}
