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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestInfluxMetadata
{
    private static TestingInfluxServer server;
    private static InfluxMetadata metadata;

    @BeforeAll
    public static void setup()
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

    @AfterAll
    public static void destroy()
    {
        server.close();
        server = null;
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).isEqualTo(ImmutableList.of(TEST_DATABASE, TEST_DATABASE_ANOTHER));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty()))).isEqualTo(ImmutableSet.of(
                new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X),
                new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_Y),
                new SchemaTableName(TEST_DATABASE_ANOTHER, TEST_MEASUREMENT_Z)));

        // specific schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of(TEST_DATABASE)))).isEqualTo(ImmutableSet.of(
                new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X),
                new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_Y)));
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of(TEST_DATABASE_ANOTHER)))).isEqualTo(ImmutableSet.of(
                new SchemaTableName(TEST_DATABASE_ANOTHER, TEST_MEASUREMENT_Z)));

        // unknown schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown")))).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testGetTableHandle()
    {
        SchemaTableName schemaTableName = new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X);
        InfluxTableHandle tableHandle = new InfluxTableHandle(TEST_DATABASE, TEST_MEASUREMENT_X);
        assertThat(metadata.getTableHandle(SESSION, schemaTableName)).isEqualTo(tableHandle);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(TEST_DATABASE, "unknown"))).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", TEST_MEASUREMENT_X))).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown"))).isNull();
    }

    @Test
    public void testGetTableMetadata()
    {
        InfluxTableHandle tableHandle = new InfluxTableHandle(TEST_DATABASE, TEST_MEASUREMENT_X);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        SchemaTableName table = tableMetadata.getTable();
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        assertThat(table).isEqualTo(new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X));
        assertThat(columns).isEqualTo(ImmutableList.of(
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
        assertThat(metadata.getColumnHandles(SESSION, tableHandle)).isEqualTo(ImmutableMap.of(
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

        assertThat(columnMetadata).isEqualTo(ColumnMetadata.builder().setName("country").setType(VARCHAR).build());
    }

    @Test
    public void testStreamTableColumns()
    {
        // known table
        SchemaTablePrefix prefix = new SchemaTablePrefix(TEST_DATABASE, TEST_MEASUREMENT_X);
        Iterator<TableColumnsMetadata> iterator = metadata.streamTableColumns(SESSION, prefix);
        assertThat(iterator).isNotNull();
        if (iterator.hasNext()) {
            TableColumnsMetadata next = iterator.next();
            assertThat(next.getTable()).isEqualTo(new SchemaTableName(TEST_DATABASE, TEST_MEASUREMENT_X));
            assertThat(next.getColumns().isEmpty()).isFalse();
            assertThat(next.getColumns().get()).isEqualTo(ImmutableList.of(
                    new ColumnMetadata("time", TIMESTAMP_NANOS),
                    new ColumnMetadata("f1", BIGINT),
                    new ColumnMetadata("f2", DOUBLE),
                    new ColumnMetadata("f3", VARCHAR),
                    new ColumnMetadata("f4", BOOLEAN),
                    new ColumnMetadata("country", VARCHAR)));
        }

        // unknown table
        iterator = metadata.streamTableColumns(SESSION, new SchemaTablePrefix(TEST_DATABASE, "unknown"));
        assertThat(iterator.hasNext()).isFalse();
        iterator = metadata.streamTableColumns(SESSION, new SchemaTablePrefix("unknown", "unknown"));
        assertThat(iterator.hasNext()).isFalse();
    }
}
