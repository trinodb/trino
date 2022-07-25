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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.Optional;

import static io.trino.plugin.example.MetadataUtil.CATALOG_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestExampleMetadata
{
    private static final ExampleTableHandle NUMBERS_TABLE_HANDLE = new ExampleTableHandle("example", "numbers");
    private ExampleMetadata metadata;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestExampleClient.class, "/example-data/example-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        ExampleClient client = new ExampleClient(new ExampleConfig().setMetadata(metadataUrl.toURI()), CATALOG_CODEC);
        metadata = new ExampleMetadata(client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableSet.of("example", "tpch"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers")), NUMBERS_TABLE_HANDLE);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, NUMBERS_TABLE_HANDLE), ImmutableMap.of(
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
        assertEquals(tableMetadata.getTable(), new SchemaTableName("example", "numbers"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("text", createUnboundedVarcharType()),
                new ColumnMetadata("value", BIGINT)));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(SESSION, new ExampleTableHandle("unknown", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new ExampleTableHandle("example", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new ExampleTableHandle("unknown", "numbers")));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty())), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("example"))), ImmutableSet.of(
                new SchemaTableName("example", "numbers")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("tpch"))), ImmutableSet.of(
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown"))), ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(metadata.getColumnMetadata(SESSION, NUMBERS_TABLE_HANDLE, new ExampleColumnHandle("text", createUnboundedVarcharType(), 0)),
                new ColumnMetadata("text", createUnboundedVarcharType()));

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
                false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("This connector does not support creating tables");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testDropTableTable()
    {
        metadata.dropTable(SESSION, NUMBERS_TABLE_HANDLE);
    }
}
