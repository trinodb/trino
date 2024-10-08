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
package io.trino.plugin.example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;
import java.util.Optional;

import static io.trino.plugin.example.MetadataUtil.CATALOG_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestExampleMetadata
{
    private static final ExampleTableHandle NUMBERS_TABLE_HANDLE = new ExampleTableHandle("example", "numbers");
    private ExampleMetadata metadata;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestExampleClient.class, "/example-data/example-metadata.json");
        assertThat(metadataUrl)
                .describedAs("metadataUrl is null")
                .isNotNull();
        ExampleClient client = new ExampleClient(new ExampleConfig().setMetadata(metadataUrl.toURI()), CATALOG_CODEC);
        metadata = new ExampleMetadata(client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsExactlyElementsOf(ImmutableSet.of("example", "tpch"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty())).isEqualTo(NUMBERS_TABLE_HANDLE);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown"), Optional.empty(), Optional.empty())).isNull();
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertThat(metadata.getColumnHandles(SESSION, NUMBERS_TABLE_HANDLE)).isEqualTo(ImmutableMap.of(
                "text", new ExampleColumnHandle("text", createUnboundedVarcharType(), 0),
                "value", new ExampleColumnHandle("value", BIGINT, 1)));

        // unknown table
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new ExampleTableHandle("unknown", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'unknown.unknown' not found");
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new ExampleTableHandle("example", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'example.unknown' not found");
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, NUMBERS_TABLE_HANDLE);
        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("example", "numbers"));
        assertThat(tableMetadata.getColumns()).isEqualTo(ImmutableList.of(
                new ColumnMetadata("text", createUnboundedVarcharType()),
                new ColumnMetadata("value", BIGINT)));

        // unknown tables should produce null
        assertThat(metadata.getTableMetadata(SESSION, new ExampleTableHandle("unknown", "unknown"))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new ExampleTableHandle("example", "unknown"))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new ExampleTableHandle("unknown", "numbers"))).isNull();
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty()))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // specific schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("example")))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("example", "numbers")));
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("tpch")))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // unknown schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown")))).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertThat(metadata.getColumnMetadata(SESSION, NUMBERS_TABLE_HANDLE, new ExampleColumnHandle("text", createUnboundedVarcharType(), 0))).isEqualTo(new ColumnMetadata("text", createUnboundedVarcharType()));

        // example connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // ExampleTableHandle and ExampleColumnHandle passed in.  This is on because
        // it is not possible for the Trino Metadata system to create the handles
        // directly.
    }

    @Test
    public void testCreateTable()
    {
        assertThatThrownBy(() -> metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("example", "foo"),
                        ImmutableList.of(new ColumnMetadata("text", createUnboundedVarcharType()))),
                SaveMode.FAIL))
                .isInstanceOf(TrinoException.class)
                .hasMessage("This connector does not support creating tables");
    }

    @Test
    public void testDropTableTable()
    {
        assertThatThrownBy(() -> metadata.dropTable(SESSION, NUMBERS_TABLE_HANDLE))
                .isInstanceOf(TrinoException.class);
    }
}
