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
package io.trino.plugin.openlineage;

import com.google.common.collect.ImmutableMap;
import io.trino.operator.RetryPolicy;
import io.trino.spi.eventlistener.ColumnInfo;
import io.trino.spi.eventlistener.MaterializedViewReferenceInfo;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.StageOutputBufferUtilization;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.eventlistener.ViewReferenceInfo;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.time.Duration.ofSeconds;

public class TrinoEventData
{
    public static final QueryIOMetadata queryIOMetadata;
    public static final QueryContext queryContext;
    public static final QueryMetadata queryMetadata;
    public static final QueryStatistics queryStatistics;
    public static final QueryCompletedEvent queryCompleteEvent;
    public static final QueryCreatedEvent queryCreatedEvent;
    public static final QueryMetadata queryMetadataWithView;
    public static final QueryCompletedEvent queryCompleteEventWithView;
    public static final QueryMetadata queryMetadataWithNestedView;
    public static final QueryCompletedEvent queryCompleteEventWithNestedView;
    public static final QueryMetadata queryMetadataWithDirectTable;
    public static final QueryCompletedEvent queryCompleteEventWithDirectTable;
    public static final QueryMetadata queryMetadataWithMixedInputs;
    public static final QueryCompletedEvent queryCompleteEventWithMixedInputs;
    public static final QueryMetadata queryMetadataWithViewMultipleBaseTables;
    public static final QueryCompletedEvent queryCompleteEventWithViewMultipleBaseTables;
    public static final QueryMetadata queryMetadataWithNestedViewComplexJoin;
    public static final QueryCompletedEvent queryCompleteEventWithNestedViewComplexJoin;
    public static final QueryMetadata queryMetadataWithFreshMaterializedView;
    public static final QueryCompletedEvent queryCompleteEventWithFreshMaterializedView;
    public static final QueryMetadata queryMetadataWithStaleMaterializedView;
    public static final QueryCompletedEvent queryCompleteEventWithStaleMaterializedView;

    private TrinoEventData()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    static {
        queryIOMetadata = new QueryIOMetadata(Collections.emptyList(), Optional.empty());

        queryContext = new QueryContext(
                "user",
                "originalUser",
                Set.of(), // originalRoles
                Optional.of("principal"),
                Set.of(), // enabledRoles
                Set.of(), // groups
                Optional.of("traceToken"),
                Optional.of("127.0.0.1"),
                Optional.of("Some-User-Agent"),
                Optional.of("Some client info"),
                new HashSet<>(), // clientTags
                new HashSet<>(), // clientCapabilities
                Optional.of("some-trino-client"),
                UTC_KEY.getId(),
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(new ResourceGroupId("name")),
                new HashMap<>(), // sessionProperties
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(1000L)),
                "serverAddress", "serverVersion", "environment",
                Optional.of(QueryType.INSERT),
                RetryPolicy.QUERY.toString());

        queryMetadata = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(), // encoding
                "create table b.c as select * from y.z",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(), // tables
                List.of(), // routines
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(), // jsonPlan
                Optional.empty()); // payload

        queryStatistics = new QueryStatistics(
                ofSeconds(1),
                ofSeconds(1),
                ofSeconds(1),
                ofSeconds(1),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0.0f,
                Collections.emptyList(),
                0,
                true,
                Collections.emptyList(),
                List.of(new StageOutputBufferUtilization(0, 10, 0.1, 0.5, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.0, 1.0, ofSeconds(1234))),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty());

        queryCompleteEvent = new QueryCompletedEvent(
                queryMetadata,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        queryCreatedEvent = new QueryCreatedEvent(
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                queryContext,
                queryMetadata);

        // View: my_view (directly referenced, has viewText)
        TableInfo viewTable = new TableInfo(
                "marquez",
                "default",
                "my_view",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("name", Optional.empty())),
                true,
                Optional.of("SELECT * FROM base_table"),
                List.of());

        // Base table: base_table (not directly referenced, resolved from view)
        TableInfo baseTable = new TableInfo(
                "marquez",
                "default",
                "base_table",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("name", Optional.empty())),
                false,
                Optional.empty(),
                List.of(new ViewReferenceInfo("marquez", "default", "my_view")));

        queryMetadataWithView = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(),
                "create table b.c as select * from my_view",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(viewTable, baseTable),
                List.of(),
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(),
                Optional.empty());

        queryCompleteEventWithView = new QueryCompletedEvent(
                queryMetadataWithView,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        // Nested view scenario: SELECT * FROM v2, where v2 -> v1 -> base_table
        // v2: directly referenced, has viewText, empty referenceChain
        TableInfo nestedViewV2 = new TableInfo(
                "marquez",
                "default",
                "v2",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("name", Optional.empty())),
                true,
                Optional.of("SELECT * FROM v1"),
                List.of());

        // v1: not directly referenced, has viewText, referenceChain points to v2
        TableInfo nestedViewV1 = new TableInfo(
                "marquez",
                "default",
                "v1",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("name", Optional.empty())),
                false,
                Optional.of("SELECT * FROM base_table"),
                List.of(new ViewReferenceInfo("marquez", "default", "v2")));

        // base_table: not directly referenced, no viewText, referenceChain points through v2 -> v1
        TableInfo nestedBaseTable = new TableInfo(
                "marquez",
                "default",
                "base_table",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("name", Optional.empty())),
                false,
                Optional.empty(),
                List.of(
                        new ViewReferenceInfo("marquez", "default", "v2"),
                        new ViewReferenceInfo("marquez", "default", "v1")));

        queryMetadataWithNestedView = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(),
                "create table b.c as select * from v2",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(nestedViewV2, nestedViewV1, nestedBaseTable),
                List.of(),
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(),
                Optional.empty());

        queryCompleteEventWithNestedView = new QueryCompletedEvent(
                queryMetadataWithNestedView,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        // Scenario: Direct table query (no views involved)
        // SELECT * FROM orders
        TableInfo directTable = new TableInfo(
                "marquez",
                "default",
                "orders",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("orderkey", Optional.empty()),
                        new ColumnInfo("custkey", Optional.empty())),
                true,
                Optional.empty(),
                List.of());

        queryMetadataWithDirectTable = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(),
                "select * from orders",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(directTable),
                List.of(),
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(),
                Optional.empty());

        queryCompleteEventWithDirectTable = new QueryCompletedEvent(
                queryMetadataWithDirectTable,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        // Scenario: Mixed inputs - query joins a direct table and a view
        // SELECT * FROM orders JOIN my_view ON ...
        // where my_view is defined as SELECT * FROM nation
        TableInfo mixedDirectTable = new TableInfo(
                "marquez",
                "default",
                "orders",
                "user",
                List.of(),
                List.of(new ColumnInfo("orderkey", Optional.empty())),
                true,
                Optional.empty(),
                List.of());

        TableInfo mixedView = new TableInfo(
                "marquez",
                "default",
                "my_view",
                "user",
                List.of(),
                List.of(new ColumnInfo("nationkey", Optional.empty())),
                true,
                Optional.of("SELECT * FROM nation"),
                List.of());

        TableInfo mixedViewBaseTable = new TableInfo(
                "marquez",
                "default",
                "nation",
                "user",
                List.of(),
                List.of(new ColumnInfo("nationkey", Optional.empty())),
                false,
                Optional.empty(),
                List.of(new ViewReferenceInfo("marquez", "default", "my_view")));

        queryMetadataWithMixedInputs = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(),
                "select * from orders join my_view on orders.orderkey = my_view.nationkey",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(mixedDirectTable, mixedView, mixedViewBaseTable),
                List.of(),
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(),
                Optional.empty());

        queryCompleteEventWithMixedInputs = new QueryCompletedEvent(
                queryMetadataWithMixedInputs,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        // Scenario: View with multiple base tables
        // SELECT * FROM join_view, where join_view = SELECT * FROM t1 JOIN t2
        TableInfo multiBaseView = new TableInfo(
                "marquez",
                "default",
                "join_view",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("col_a", Optional.empty()),
                        new ColumnInfo("col_b", Optional.empty())),
                true,
                Optional.of("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"),
                List.of());

        TableInfo multiBaseT1 = new TableInfo(
                "marquez",
                "default",
                "t1",
                "user",
                List.of(),
                List.of(new ColumnInfo("col_a", Optional.empty())),
                false,
                Optional.empty(),
                List.of(new ViewReferenceInfo("marquez", "default", "join_view")));

        TableInfo multiBaseT2 = new TableInfo(
                "marquez",
                "default",
                "t2",
                "user",
                List.of(),
                List.of(new ColumnInfo("col_b", Optional.empty())),
                false,
                Optional.empty(),
                List.of(new ViewReferenceInfo("marquez", "default", "join_view")));

        queryMetadataWithViewMultipleBaseTables = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(),
                "select * from join_view",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(multiBaseView, multiBaseT1, multiBaseT2),
                List.of(),
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(),
                Optional.empty());

        queryCompleteEventWithViewMultipleBaseTables = new QueryCompletedEvent(
                queryMetadataWithViewMultipleBaseTables,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        // Scenario: 2-level nested view with complex joins
        // SELECT * FROM v_outer
        // v_outer = SELECT * FROM v_inner JOIN orders ON v_inner.nationkey = orders.orderkey
        // v_inner = SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey
        // Resolved base tables: nation, region (depth 2), orders (depth 1)

        // v_outer: directly referenced, has viewText, referenceChain = []
        TableInfo complexOuterView = new TableInfo(
                "marquez",
                "default",
                "v_outer",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("regionkey", Optional.empty()),
                        new ColumnInfo("orderkey", Optional.empty())),
                true,
                Optional.of("SELECT * FROM v_inner JOIN orders ON v_inner.nationkey = orders.orderkey"),
                List.of());

        // v_inner: not directly referenced, has viewText, referenceChain = [v_outer]
        TableInfo complexInnerView = new TableInfo(
                "marquez",
                "default",
                "v_inner",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("regionkey", Optional.empty())),
                false,
                Optional.of("SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey"),
                List.of(new ViewReferenceInfo("marquez", "default", "v_outer")));

        // orders: not directly referenced, no viewText, referenceChain = [v_outer] (depth 1)
        TableInfo complexOrders = new TableInfo(
                "marquez",
                "default",
                "orders",
                "user",
                List.of(),
                List.of(new ColumnInfo("orderkey", Optional.empty())),
                false,
                Optional.empty(),
                List.of(new ViewReferenceInfo("marquez", "default", "v_outer")));

        // nation: not directly referenced, no viewText, referenceChain = [v_outer, v_inner] (depth 2)
        TableInfo complexNation = new TableInfo(
                "marquez",
                "default",
                "nation",
                "user",
                List.of(),
                List.of(new ColumnInfo("nationkey", Optional.empty())),
                false,
                Optional.empty(),
                List.of(
                        new ViewReferenceInfo("marquez", "default", "v_outer"),
                        new ViewReferenceInfo("marquez", "default", "v_inner")));

        // region: not directly referenced, no viewText, referenceChain = [v_outer, v_inner] (depth 2)
        TableInfo complexRegion = new TableInfo(
                "marquez",
                "default",
                "region",
                "user",
                List.of(),
                List.of(new ColumnInfo("regionkey", Optional.empty())),
                false,
                Optional.empty(),
                List.of(
                        new ViewReferenceInfo("marquez", "default", "v_outer"),
                        new ViewReferenceInfo("marquez", "default", "v_inner")));

        queryMetadataWithNestedViewComplexJoin = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(),
                "select * from v_outer",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(complexOuterView, complexInnerView, complexOrders, complexNation, complexRegion),
                List.of(),
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(),
                Optional.empty());

        queryCompleteEventWithNestedViewComplexJoin = new QueryCompletedEvent(
                queryMetadataWithNestedViewComplexJoin,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        // Scenario: Fresh materialized view (storage table is up-to-date)
        // SELECT * FROM mv_fresh
        // Only the MV entry is in the tables list — no base tables resolved
        TableInfo freshMvTable = new TableInfo(
                "marquez",
                "default",
                "mv_fresh",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("name", Optional.empty())),
                true,
                Optional.of("SELECT * FROM nation"),
                List.of());

        queryMetadataWithFreshMaterializedView = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(),
                "select * from mv_fresh",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(freshMvTable),
                List.of(),
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(),
                Optional.empty());

        queryCompleteEventWithFreshMaterializedView = new QueryCompletedEvent(
                queryMetadataWithFreshMaterializedView,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        // Scenario: Stale materialized view (storage table is outdated, query falls back to base tables)
        // SELECT * FROM mv_stale
        // MV entry + base table entry with MaterializedViewReferenceInfo in referenceChain
        TableInfo staleMvTable = new TableInfo(
                "marquez",
                "default",
                "mv_stale",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("name", Optional.empty())),
                true,
                Optional.of("SELECT * FROM nation"),
                List.of());

        TableInfo staleMvBaseTable = new TableInfo(
                "marquez",
                "default",
                "nation",
                "user",
                List.of(),
                List.of(
                        new ColumnInfo("nationkey", Optional.empty()),
                        new ColumnInfo("name", Optional.empty())),
                false,
                Optional.empty(),
                List.of(new MaterializedViewReferenceInfo("marquez", "default", "mv_stale")));

        queryMetadataWithStaleMaterializedView = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(),
                "select * from mv_stale",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(staleMvTable, staleMvBaseTable),
                List.of(),
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(),
                Optional.empty());

        queryCompleteEventWithStaleMaterializedView = new QueryCompletedEvent(
                queryMetadataWithStaleMaterializedView,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));
    }
}
