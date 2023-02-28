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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind.FIELD;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind.TAG;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnName;
import static io.trino.plugin.influxdb.InfluxDataTool.TEST_DATABASE;
import static io.trino.plugin.influxdb.InfluxDataTool.TEST_DATABASE_ANOTHER;
import static io.trino.plugin.influxdb.InfluxDataTool.TEST_MEASUREMENT_X;
import static io.trino.plugin.influxdb.InfluxDataTool.TEST_MEASUREMENT_Y;
import static io.trino.plugin.influxdb.InfluxDataTool.TEST_MEASUREMENT_Z;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestInfluxMetadata
{
    private TestingInfluxServer server;
    private InfluxMetadata metadata;

    @BeforeClass
    public void setup()
    {
        server = new TestingInfluxServer();
        InfluxConfig config = new InfluxConfig();
        config.setEndpoint(server.getEndpoint());
        config.setUsername(TestingInfluxServer.USERNAME);
        config.setPassword(TestingInfluxServer.PASSWORD);
        metadata = new InfluxMetadata(new NativeInfluxClient(config));

        try (InfluxSession session = new InfluxSession(server.getEndpoint())) {
            InfluxDataTool tool = new InfluxDataTool(session);
            tool.setUpDatabase();
            tool.setUpDataForTest();
        }
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
        server = null;
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of(TEST_DATABASE, TEST_DATABASE_ANOTHER));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty())), ImmutableSet.of(
                new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X),
                new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_Y),
                new SchemaTableName(TEST_DATABASE_ANOTHER, TEST_MEASUREMENT_Z)));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of(TEST_DATABASE))), ImmutableSet.of(
                new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X),
                new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_Y)));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of(TEST_DATABASE_ANOTHER))), ImmutableSet.of(
                new SchemaTableName(TEST_DATABASE_ANOTHER, TEST_MEASUREMENT_Z)));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown"))), ImmutableSet.of());
    }

    @Test
    public void testGetTableHandle()
    {
        SchemaTableName schemaTableName = new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X);
        InfluxTableHandle tableHandle = new InfluxTableHandle(TEST_DATABASE, TEST_MEASUREMENT_X);
        assertEquals(metadata.getTableHandle(SESSION, schemaTableName), tableHandle);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName(TEST_DATABASE, "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", TEST_MEASUREMENT_X)));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetTableMetadata()
    {
        InfluxTableHandle tableHandle = new InfluxTableHandle(TEST_DATABASE, TEST_MEASUREMENT_X);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        SchemaTableName table = tableMetadata.getTable();
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        assertEquals(table, new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X));
        assertEquals(columns, ImmutableList.of(
                new ColumnMetadata(ColumnName.TIME.getName(), TIMESTAMP_NANOS),
                new ColumnMetadata("f1", BIGINT),
                new ColumnMetadata("f2", DOUBLE),
                new ColumnMetadata("f3", VARCHAR),
                new ColumnMetadata("f4", BOOLEAN),
                new ColumnMetadata("country", VARCHAR)));

        assertThatThrownBy(() -> metadata.getTableMetadata(SESSION, new InfluxTableHandle("unknown", "unknown")))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("The table handle is invalid " + new InfluxTableHandle("unknown", "unknown"));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        InfluxTableHandle tableHandle = new InfluxTableHandle(TEST_DATABASE, TEST_MEASUREMENT_X);
        assertEquals(metadata.getColumnHandles(SESSION, tableHandle), ImmutableMap.of(
                "time", new InfluxColumnHandle("time", TIMESTAMP_NANOS, ColumnKind.TIME),
                "f1", new InfluxColumnHandle("f1", BIGINT, FIELD),
                "f2", new InfluxColumnHandle("f2", DOUBLE, FIELD),
                "f3", new InfluxColumnHandle("f3", VARCHAR, FIELD),
                "f4", new InfluxColumnHandle("f4", BOOLEAN, FIELD),
                "country", new InfluxColumnHandle("country", VARCHAR, TAG)));

        // unknown table
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new InfluxTableHandle(TEST_DATABASE, "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table '" + TEST_DATABASE + ".unknown' not found");
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new InfluxTableHandle("unknown", TEST_MEASUREMENT_X)))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table '" + "unknown." + TEST_MEASUREMENT_X + "' not found");
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new InfluxTableHandle("unknown", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'unknown.unknown' not found");
    }

    @Test
    public void testGetColumnMetadata()
    {
        InfluxTableHandle tableHandle = new InfluxTableHandle(TEST_DATABASE, TEST_MEASUREMENT_X);
        InfluxColumnHandle columnHandle = new InfluxColumnHandle("country", VARCHAR, TAG);
        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);

        assertEquals(columnMetadata, ColumnMetadata.builder().setName("country").setType(VARCHAR).build());
    }

    @Test
    public void testStreamTableColumns()
    {
        // known table
        SchemaTablePrefix prefix = new SchemaTablePrefix(TEST_DATABASE, TEST_MEASUREMENT_X);
        Iterator<TableColumnsMetadata> iterator = metadata.streamTableColumns(SESSION, prefix);
        assertNotNull(iterator);
        if (iterator.hasNext()) {
            TableColumnsMetadata next = iterator.next();
            assertEquals(next.getTable(), new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X));
            assertFalse(next.getColumns().isEmpty());
            assertEquals(next.getColumns().get(), ImmutableList.of(
                    new ColumnMetadata("time", TIMESTAMP_NANOS),
                    new ColumnMetadata("f1", BIGINT),
                    new ColumnMetadata("f2", DOUBLE),
                    new ColumnMetadata("f3", VARCHAR),
                    new ColumnMetadata("f4", BOOLEAN),
                    new ColumnMetadata("country", VARCHAR)));
        }

        // unknown table
        assertEquals(metadata.streamTableColumns(SESSION, new SchemaTablePrefix(TEST_DATABASE, "unknown")),
                ImmutableMap.of().entrySet().stream().iterator());
        assertEquals(metadata.streamTableColumns(SESSION, new SchemaTablePrefix("unknown", "unknown")),
                ImmutableMap.of().entrySet().stream().iterator());
    }
}
