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
package io.trino.plugin.starrocks;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeSignature;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStarRocksMetadata
{
    private static final class TestingStarRocksMetadataClient
            implements StarRocksMetadataClient
    {
        private final Map<SchemaTableName, StarRocksRemoteTable> tables;

        private TestingStarRocksMetadataClient(List<StarRocksRemoteTable> tables)
        {
            this.tables = tables.stream()
                    .collect(Collectors.toUnmodifiableMap(StarRocksRemoteTable::schemaTableName, table -> table));
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            return tables.keySet().stream()
                    .map(SchemaTableName::getSchemaName)
                    .distinct()
                    .toList();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
        {
            return tables.keySet().stream()
                    .filter(table -> schemaName.isEmpty() || table.getSchemaName().equals(schemaName.orElseThrow()))
                    .toList();
        }

        @Override
        public Optional<StarRocksRemoteTable> getTable(ConnectorSession session, SchemaTableName tableName)
        {
            return Optional.ofNullable(tables.get(tableName));
        }

        @Override
        public OptionalLong getTableRowCount(ConnectorSession session, SchemaTableName tableName)
        {
            return tables.containsKey(tableName) ? OptionalLong.of(42) : OptionalLong.empty();
        }
    }

    private static final StarRocksRemoteTable EVENTS = new StarRocksRemoteTable(
            new SchemaTableName("analytics", "events"),
            Optional.empty(),
            "analytics",
            "events",
            StarRocksRelationType.TABLE,
            List.of(
                    new StarRocksRemoteColumn("event_id", "event_id", "BIGINT", Optional.of(20), Optional.empty(), 1, Optional.empty()),
                    new StarRocksRemoteColumn("created_at", "created_at", "DATETIME", Optional.empty(), Optional.empty(), 2, Optional.of("datetimev2(3)")),
                    new StarRocksRemoteColumn("payload", "payload", "JSON", Optional.empty(), Optional.empty(), 3, Optional.of("json"))));

    private static final StarRocksRemoteTable METRICS_VIEW = new StarRocksRemoteTable(
            new SchemaTableName("analytics", "metrics_view"),
            Optional.empty(),
            "Analytics",
            "MetricsView",
            StarRocksRelationType.VIEW,
            List.of(
                    new StarRocksRemoteColumn("metric_key", "metric_key", "BIGINT", Optional.of(20), Optional.empty(), 1, Optional.empty()),
                    new StarRocksRemoteColumn("largeint_summary", "largeint_summary", "DECIMAL", Optional.empty(), Optional.empty(), 2, Optional.of("largeint")),
                    new StarRocksRemoteColumn("amount", "amount", "DECIMAL", Optional.of(18), Optional.of(2), 3, Optional.of("decimal(18,2)")),
                    new StarRocksRemoteColumn("hll_state", "hll_state", "HLL", Optional.empty(), Optional.empty(), 4, Optional.of("hll"))));

    private final StarRocksMetadata metadata = new StarRocksMetadata(
            new TestingStarRocksMetadataClient(List.of(EVENTS, METRICS_VIEW)),
            new StarRocksTypeMapper(TESTING_TYPE_MANAGER));

    @Test
    void testListSchemaNamesAndTables()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsExactly("analytics");
        assertThat(metadata.listTables(SESSION, Optional.empty())).containsExactlyInAnyOrder(EVENTS.schemaTableName(), METRICS_VIEW.schemaTableName());
        assertThat(metadata.listTables(SESSION, Optional.of("analytics"))).containsExactlyInAnyOrder(EVENTS.schemaTableName(), METRICS_VIEW.schemaTableName());
    }

    @Test
    void testGetTableHandleAndMetadata()
    {
        StarRocksTableHandle tableHandle = (StarRocksTableHandle) metadata.getTableHandle(SESSION, EVENTS.schemaTableName(), Optional.empty(), Optional.empty());

        assertThat(tableHandle).isEqualTo(new StarRocksTableHandle("analytics", "events", Optional.empty(), "analytics", "events", StarRocksRelationType.TABLE));
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("analytics", "missing"), Optional.empty(), Optional.empty())).isNull();

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        assertThat(tableMetadata.getTable()).isEqualTo(EVENTS.schemaTableName());
        assertThat(tableMetadata.getColumns()).isEqualTo(List.of(
                new ColumnMetadata("event_id", BIGINT),
                new ColumnMetadata("created_at", createTimestampType(3)),
                new ColumnMetadata("payload", TESTING_TYPE_MANAGER.getType(new TypeSignature(JSON)))));
    }

    @Test
    void testRemoteNamesAndViewRelationTypeArePreserved()
    {
        StarRocksTableHandle tableHandle = (StarRocksTableHandle) metadata.getTableHandle(SESSION, METRICS_VIEW.schemaTableName(), Optional.empty(), Optional.empty());

        assertThat(tableHandle.remoteSchemaName()).isEqualTo("Analytics");
        assertThat(tableHandle.remoteTableName()).isEqualTo("MetricsView");
        assertThat(tableHandle.relationType()).isEqualTo(StarRocksRelationType.VIEW);
    }

    @Test
    void testColumnHandlesAndTableColumns()
    {
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, new StarRocksTableHandle("analytics", "metrics_view", Optional.empty(), "Analytics", "MetricsView", StarRocksRelationType.VIEW));

        assertThat(columnHandles).isEqualTo(Map.of(
                "metric_key", new StarRocksColumnHandle("metric_key", "metric_key", BIGINT, 0),
                "largeint_summary", new StarRocksColumnHandle("largeint_summary", "largeint_summary", createUnboundedVarcharType(), 1),
                "amount", new StarRocksColumnHandle("amount", "amount", createDecimalType(18, 2), 2)));

        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("analytics"));
        assertThat(tableColumns).isEqualTo(Map.of(
                EVENTS.schemaTableName(), List.of(
                        new ColumnMetadata("event_id", BIGINT),
                        new ColumnMetadata("created_at", createTimestampType(3)),
                        new ColumnMetadata("payload", TESTING_TYPE_MANAGER.getType(new TypeSignature(JSON)))),
                METRICS_VIEW.schemaTableName(), List.of(
                        new ColumnMetadata("metric_key", BIGINT),
                        new ColumnMetadata("largeint_summary", createUnboundedVarcharType()),
                        new ColumnMetadata("amount", createDecimalType(18, 2)))));
    }

    @Test
    void testTableStatisticsUsesRemoteRowCount()
    {
        assertThat(metadata.getTableStatistics(SESSION, new StarRocksTableHandle("analytics", "events", Optional.empty(), "analytics", "events", StarRocksRelationType.TABLE)).getRowCount().getValue())
                .isEqualTo(42.0);
    }

    @Test
    void testDoesNotPushFilterOrTopNAcrossExistingLimitBoundary()
    {
        StarRocksColumnHandle eventId = new StarRocksColumnHandle("event_id", "event_id", BIGINT, 0);
        StarRocksTableHandle table = new StarRocksTableHandle("analytics", "events", Optional.empty(), "analytics", "events", StarRocksRelationType.TABLE);
        StarRocksTableHandle limitedTable = table.withTopN(10, List.of(new StarRocksSortItem("event_id", "event_id", ASC_NULLS_LAST)));

        assertThat(metadata.applyFilter(
                SESSION,
                limitedTable,
                new Constraint(TupleDomain.withColumnDomains(Map.of(eventId, Domain.singleValue(BIGINT, 1L))))))
                .isEmpty();

        assertThat(metadata.applyTopN(
                SESSION,
                table.withLimit(10),
                5,
                List.of(new SortItem("event_id", ASC_NULLS_LAST)),
                Map.of("event_id", eventId)))
                .isEmpty();
    }
}
