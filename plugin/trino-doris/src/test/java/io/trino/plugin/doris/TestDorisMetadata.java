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
package io.trino.plugin.doris;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisMetadata
{
    private static final class TestingDorisMetadataClient
            implements DorisMetadataClient
    {
        private final Map<SchemaTableName, DorisRemoteTable> tables;

        private TestingDorisMetadataClient(List<DorisRemoteTable> tables)
        {
            this.tables = tables.stream()
                    .collect(Collectors.toUnmodifiableMap(DorisRemoteTable::schemaTableName, table -> table));
        }

        @Override
        public List<String> listSchemaNames()
        {
            return tables.keySet().stream()
                    .map(SchemaTableName::getSchemaName)
                    .distinct()
                    .toList();
        }

        @Override
        public List<SchemaTableName> listTables(Optional<String> schemaName)
        {
            return tables.keySet().stream()
                    .filter(table -> schemaName.isEmpty() || table.getSchemaName().equals(schemaName.orElseThrow()))
                    .toList();
        }

        @Override
        public Optional<DorisRemoteTable> getTable(SchemaTableName tableName)
        {
            return Optional.ofNullable(tables.get(tableName));
        }

        @Override
        public OptionalLong getTableRowCount(SchemaTableName tableName)
        {
            return tables.containsKey(tableName) ? OptionalLong.of(1000) : OptionalLong.empty();
        }
    }

    private static final DorisRemoteTable ORDERS = new DorisRemoteTable(
            new SchemaTableName("sales", "orders"),
            List.of(
                    new DorisRemoteColumn("id", "BIGINT", Optional.of(20), Optional.empty(), 1),
                    new DorisRemoteColumn("event_time", "DATETIME_V2", Optional.empty(), Optional.of(6), 2),
                    new DorisRemoteColumn("payload", "JSON", Optional.empty(), Optional.empty(), 3),
                    new DorisRemoteColumn("score", "FLOAT", Optional.empty(), Optional.empty(), 4)));

    private static final DorisRemoteTable EVENTS = new DorisRemoteTable(
            new SchemaTableName("ops", "events"),
            List.of(
                    new DorisRemoteColumn("event_id", "BIGINT UNSIGNED", Optional.of(20), Optional.of(0), 1),
                    new DorisRemoteColumn("details", "STRING", Optional.empty(), Optional.empty(), 2)));

    private final DorisMetadata metadata = new DorisMetadata(
            new TestingDorisMetadataClient(List.of(ORDERS, EVENTS)),
            new DorisTypeMapper(new DorisConfig()));

    @Test
    void testListSchemaNamesAndTables()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsExactlyInAnyOrder("sales", "ops");
        assertThat(metadata.listTables(SESSION, Optional.empty())).containsExactlyInAnyOrder(ORDERS.schemaTableName(), EVENTS.schemaTableName());
        assertThat(metadata.listTables(SESSION, Optional.of("sales"))).containsExactly(ORDERS.schemaTableName());
        assertThat(metadata.listTables(SESSION, Optional.of("missing"))).isEmpty();
    }

    @Test
    void testGetTableHandleAndTableMetadata()
    {
        DorisTableHandle tableHandle = (DorisTableHandle) metadata.getTableHandle(SESSION, ORDERS.schemaTableName(), Optional.empty(), Optional.empty());
        assertThat(tableHandle).isEqualTo(new DorisTableHandle("sales", "orders"));
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("sales", "missing"), Optional.empty(), Optional.empty())).isNull();

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        assertThat(tableMetadata.getTable()).isEqualTo(ORDERS.schemaTableName());
        assertThat(tableMetadata.getColumns()).isEqualTo(List.of(
                new ColumnMetadata("id", BIGINT),
                new ColumnMetadata("event_time", createTimestampType(6)),
                new ColumnMetadata("payload", createUnboundedVarcharType()),
                new ColumnMetadata("score", REAL)));
    }

    @Test
    void testGetTableHandlePreservesRemoteCaseForReads()
    {
        DorisRemoteTable mixedCaseOrders = new DorisRemoteTable(
                new SchemaTableName("sales", "orders"),
                "Sales",
                "Orders",
                ORDERS.columns());
        DorisMetadata mixedCaseMetadata = new DorisMetadata(
                new TestingDorisMetadataClient(List.of(mixedCaseOrders)),
                new DorisTypeMapper(new DorisConfig()));

        DorisTableHandle tableHandle = (DorisTableHandle) mixedCaseMetadata.getTableHandle(SESSION, new SchemaTableName("sales", "orders"), Optional.empty(), Optional.empty());

        assertThat(tableHandle.schemaName()).isEqualTo("sales");
        assertThat(tableHandle.tableName()).isEqualTo("orders");
        assertThat(tableHandle.remoteSchemaName()).isEqualTo("Sales");
        assertThat(tableHandle.remoteTableName()).isEqualTo("Orders");
    }

    @Test
    void testGetViewHandlePreservesViewRelationType()
    {
        DorisRemoteTable revenueView = new DorisRemoteTable(
                new SchemaTableName("tpch", "revenue0"),
                "tpch",
                "revenue0",
                DorisRelationType.VIEW,
                List.of(
                        new DorisRemoteColumn("supplier_no", "BIGINT", Optional.of(20), Optional.empty(), 1),
                        new DorisRemoteColumn("total_revenue", "DECIMAL", Optional.of(15), Optional.of(4), 2)));
        DorisMetadata viewMetadata = new DorisMetadata(
                new TestingDorisMetadataClient(List.of(revenueView)),
                new DorisTypeMapper(new DorisConfig()));

        DorisTableHandle tableHandle = (DorisTableHandle) viewMetadata.getTableHandle(SESSION, revenueView.schemaTableName(), Optional.empty(), Optional.empty());

        assertThat(tableHandle.relationType()).isEqualTo(DorisRelationType.VIEW);
        assertThat(tableHandle.remoteTableName()).isEqualTo("revenue0");
    }

    @Test
    void testGetColumnHandlesAndListTableColumns()
    {
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, new DorisTableHandle("ops", "events"));
        assertThat(columnHandles).isEqualTo(Map.of(
                "event_id", new DorisColumnHandle("event_id", createDecimalType(20), 0),
                "details", new DorisColumnHandle("details", createUnboundedVarcharType(), 1)));

        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("sales"));
        assertThat(tableColumns).isEqualTo(Map.of(
                ORDERS.schemaTableName(), List.of(
                        new ColumnMetadata("id", BIGINT),
                        new ColumnMetadata("event_time", createTimestampType(6)),
                        new ColumnMetadata("payload", createUnboundedVarcharType()),
                        new ColumnMetadata("score", REAL))));
    }

    @Test
    void testTableStatisticsUsesRemoteRowCount()
    {
        assertThat(metadata.getTableStatistics(SESSION, new DorisTableHandle("sales", "orders")).getRowCount().getValue())
                .isEqualTo(1000.0);
    }
}
