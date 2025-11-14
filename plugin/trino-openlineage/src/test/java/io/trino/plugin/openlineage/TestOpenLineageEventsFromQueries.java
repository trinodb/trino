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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.BaseEvent;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsAdditional;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineageClient;
import io.trino.SessionRepresentation;
import io.trino.testing.QueryRunner;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static io.openlineage.client.OpenLineage.RunEvent.EventType.COMPLETE;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.START;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenLineageEventsFromQueries
        extends BaseTestOpenLineageQueries
{
    private final OpenLineage openLineage = new OpenLineage(URI.create("https://github.com/trinodb/trino/plugin/trino-openlineage"));
    private final OpenLineageMemoryTransport openLineageMemoryTransport = new OpenLineageMemoryTransport();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OpenLineageListenerQueryRunner.builder()
                .setCustomEventListener(new OpenLineageListener(
                        openLineage,
                        new OpenLineageClient(openLineageMemoryTransport),
                        new OpenLineageListenerConfig()
                                .setTrinoURI(URI.create(TRINO_URI))))
                .build();
    }

    @Override
    public void assertCreateTableAsSelectFromTable(String queryId, String query, String fullTableName, LineageTestTableType tableType, SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(2);

        assertThat(processedEvents.getFirst())
                .isInstanceOf(RunEvent.class);
        RunEvent startEvent = (RunEvent) processedEvents.getFirst();
        assertStartEvent(startEvent, now);
        assertStartEventRun(startEvent, queryId, tableType.ctasQueryType().name(), session);
        assertEventJob(startEvent, queryId, query);

        assertThat(processedEvents.get(1))
                .isInstanceOf(RunEvent.class);
        RunEvent completedEvent = (RunEvent) processedEvents.get(1);
        assertCompletedEvent(completedEvent, now, 1);
        assertCompletedEventRun(completedEvent, now, queryId, tableType, session);
        assertEventJob(completedEvent, queryId, query);
        assertThat(completedEvent.getInputs())
                .hasSize(1);
        assertCompletedEventInput(
                completedEvent.getInputs().getFirst(),
                "tpch.tiny.nation",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("regionkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("comment", null, null, null, null)));

        assertThat(completedEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedColumnLineage = ImmutableMap.of(
                "nationkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("nationkey")
                                .build()))
                        .build(),
                "name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("name")
                                .build()))
                        .build(),
                "regionkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("regionkey")
                                .build()))
                        .build(),
                "comment", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("comment")
                                .build()))
                        .build());
        List<InputField> expectedColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "regionkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "name", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "comment", null));
        List<SchemaDatasetFacetFields> expectedSchemaFields = List.of(
                openLineage.newSchemaDatasetFacetFields("nationkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("name", "varchar(25)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("regionkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("comment", "varchar(152)", null, null, null));
        assertCompletedEventOutput(
                completedEvent.getOutputs().getFirst(),
                fullTableName,
                expectedColumnLineage,
                expectedColumnLineageDataset,
                expectedSchemaFields,
                tableType);
        tearDown();
    }

    @Override
    public void assertCreateTableAsSelectFromView(
            String createViewQueryId,
            String createViewQuery,
            String createTableQueryId,
            String createTableQuery,
            String viewName,
            String fullTableName,
            LineageTestTableType tableType,
            SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(4);

        assertStartCompleteEventOrder(processedEvents);
        RunEvent startCreateView = getEventOfTypeAtIndex(processedEvents, START, 0);
        assertStartEvent(startCreateView, now);
        assertStartEventRun(startCreateView, createViewQueryId, "DATA_DEFINITION", session);
        assertEventJob(startCreateView, createViewQueryId, createViewQuery);

        RunEvent startCreateTableEvent = getEventOfTypeAtIndex(processedEvents, START, 1);
        assertStartEvent(startCreateTableEvent, now);
        assertStartEventRun(startCreateTableEvent, createTableQueryId, tableType.ctasQueryType().name(), session);
        assertEventJob(startCreateTableEvent, createTableQueryId, createTableQuery);

        RunEvent completeCreateViewEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 0);
        assertCompletedEvent(completeCreateViewEvent, now, 1);

        RunEvent completeCreateTableEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 1);
        assertCompletedEvent(completeCreateTableEvent, now, 1);
        assertCompletedEventRun(completeCreateTableEvent, now, createTableQueryId, tableType, session);
        assertEventJob(completeCreateTableEvent, createTableQueryId, createTableQuery);

        assertThat(completeCreateTableEvent.getInputs())
                .hasSize(1);
        assertCompletedEventInput(
                completeCreateTableEvent.getInputs().getFirst(),
                format("marquez.default.%s", viewName),
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("regionkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("comment", null, null, null, null)));

        assertThat(completeCreateTableEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedColumnLineage = ImmutableMap.of(
                "nationkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name(format("marquez.default.%s", viewName))
                                .field("nationkey")
                                .build()))
                        .build(),
                "name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name(format("marquez.default.%s", viewName))
                                .field("name")
                                .build()))
                        .build(),
                "regionkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name(format("marquez.default.%s", viewName))
                                .field("regionkey")
                                .build()))
                        .build(),
                "comment", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name(format("marquez.default.%s", viewName))
                                .field("comment")
                                .build()))
                        .build());
        List<InputField> expectedColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "regionkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "name", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "comment", null));
        List<SchemaDatasetFacetFields> expectedSchemaFields = List.of(
                openLineage.newSchemaDatasetFacetFields("nationkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("name", "varchar(25)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("regionkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("comment", "varchar(152)", null, null, null));
        assertCompletedEventOutput(
                completeCreateTableEvent.getOutputs().getFirst(),
                fullTableName,
                expectedColumnLineage,
                expectedColumnLineageDataset,
                expectedSchemaFields,
                tableType);
        tearDown();
    }

    @Override
    public void assertCreateTableWithJoin(String createTableQueryId, String createTableQuery, SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(2);

        assertThat(processedEvents.getFirst())
                .isInstanceOf(RunEvent.class);
        RunEvent startEvent = (RunEvent) processedEvents.getFirst();
        assertStartEvent(startEvent, now);
        assertStartEventRun(startEvent, createTableQueryId, "INSERT", session);
        assertEventJob(startEvent, createTableQueryId, createTableQuery);

        assertThat(processedEvents.get(1))
                .isInstanceOf(RunEvent.class);
        RunEvent completedEvent = (RunEvent) processedEvents.get(1);
        assertCompletedEvent(completedEvent, now, 3);
        assertCompletedEventRun(completedEvent, now, createTableQueryId, session);
        assertEventJob(completedEvent, createTableQueryId, createTableQuery);
        assertThat(completedEvent.getInputs())
                .hasSize(3);
        assertCompletedEventInput(
                completedEvent.getInputs().getFirst(),
                "tpch.tiny.nation",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(1),
                "tpch.sf1.customer",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("custkey", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(2),
                "tpch.tiny.orders",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("custkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("totalprice", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("orderdate", null, null, null, null)));

        assertThat(completedEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedColumnLineage = ImmutableMap.of(
                "nation", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("name")
                                .build()))
                        .build(),
                "order_count", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of())
                        .build(),
                "total_revenue", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.orders")
                                .field("totalprice")
                                .build()))
                        .build(),
                "avg_order_value", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.orders")
                                .field("totalprice")
                                .build()))
                        .build());
        List<InputField> expectedColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.sf1.customer", "custkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.sf1.customer", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.orders", "totalprice", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.orders", "orderdate", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.orders", "custkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "name", null));
        List<SchemaDatasetFacetFields> expectedSchemaFields = ImmutableList.of(
                openLineage.newSchemaDatasetFacetFields("nation", "varchar(25)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("order_count", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("total_revenue", "double", null, null, null),
                openLineage.newSchemaDatasetFacetFields("avg_order_value", "double", null, null, null));
        assertCompletedEventOutput(
                completedEvent.getOutputs().getFirst(),
                "marquez.default.test_create_table_with_join",
                expectedColumnLineage,
                expectedColumnLineageDataset,
                expectedSchemaFields);
    }

    @Override
    public void assertCreateTableWithCTE(String createTableQueryId, String createTableQuery, SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(2);

        assertThat(processedEvents.getFirst())
                .isInstanceOf(RunEvent.class);
        RunEvent startEvent = (RunEvent) processedEvents.getFirst();
        assertStartEvent(startEvent, now);
        assertStartEventRun(startEvent, createTableQueryId, "INSERT", session);
        assertEventJob(startEvent, createTableQueryId, createTableQuery);

        assertThat(processedEvents.get(1))
                .isInstanceOf(RunEvent.class);
        RunEvent completedEvent = (RunEvent) processedEvents.get(1);
        assertCompletedEvent(completedEvent, now, 3);
        assertCompletedEventRun(completedEvent, now, createTableQueryId, session);
        assertEventJob(completedEvent, createTableQueryId, createTableQuery);
        assertThat(completedEvent.getInputs())
                .hasSize(3);
        assertCompletedEventInput(
                completedEvent.getInputs().getFirst(),
                "tpcds.tiny.store_sales",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("ss_sold_date_sk", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("ss_store_sk", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("ss_sales_price", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(1),
                "tpcds.tiny.date_dim",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("d_moy", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("d_date_sk", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("d_year", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(2),
                "tpcds.tiny.store",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("s_store_name", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("s_store_sk", null, null, null, null)));

        assertThat(completedEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedColumnLineage = ImmutableMap.of(
                "d_year", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpcds.tiny.date_dim")
                                .field("d_year")
                                .build()))
                        .build(),
                "d_moy", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpcds.tiny.date_dim")
                                .field("d_moy")
                                .build()))
                        .build(),
                "year_month", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpcds.tiny.date_dim")
                                        .field("d_year")
                                        .build(),
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpcds.tiny.date_dim")
                                        .field("d_moy")
                                        .build()))
                        .build(),
                "s_store_name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpcds.tiny.store")
                                .field("s_store_name")
                                .build()))
                        .build(),
                "monthly_total", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpcds.tiny.store_sales")
                                .field("ss_sales_price")
                                .build()))
                        .build(),
                "store_rank", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpcds.tiny.date_dim")
                                        .field("d_year")
                                        .build(),
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpcds.tiny.date_dim")
                                        .field("d_moy")
                                        .build(),
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpcds.tiny.store_sales")
                                        .field("ss_sales_price")
                                        .build()))
                        .build());
        List<InputField> expectedColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.store_sales", "ss_sales_price", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.store_sales", "ss_store_sk", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.store_sales", "ss_sold_date_sk", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.date_dim", "d_date_sk", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.date_dim", "d_moy", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.date_dim", "d_year", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.store", "s_store_sk", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.store", "s_store_name", null));
        List<SchemaDatasetFacetFields> expectedSchemaFields = ImmutableList.of(
                openLineage.newSchemaDatasetFacetFields("d_year", "integer", null, null, null),
                openLineage.newSchemaDatasetFacetFields("d_moy", "integer", null, null, null),
                openLineage.newSchemaDatasetFacetFields("year_month", "varchar", null, null, null),
                openLineage.newSchemaDatasetFacetFields("s_store_name", "varchar(50)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("monthly_total", "decimal(38,2)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("store_rank", "bigint", null, null, null));
        assertCompletedEventOutput(
                completedEvent.getOutputs().getFirst(),
                "marquez.default.monthly_store_rankings",
                expectedColumnLineage,
                expectedColumnLineageDataset,
                expectedSchemaFields);
    }

    @Override
    public void assertCreateTableWithSubquery(String createTableQueryId, String createTableQuery, SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(2);

        assertThat(processedEvents.getFirst())
                .isInstanceOf(RunEvent.class);
        RunEvent startEvent = (RunEvent) processedEvents.getFirst();
        assertStartEvent(startEvent, now);
        assertStartEventRun(startEvent, createTableQueryId, "INSERT", session);
        assertEventJob(startEvent, createTableQueryId, createTableQuery);

        assertThat(processedEvents.get(1))
                .isInstanceOf(RunEvent.class);
        RunEvent completedEvent = (RunEvent) processedEvents.get(1);
        assertCompletedEvent(completedEvent, now, 4);
        assertCompletedEventRun(completedEvent, now, createTableQueryId, session);
        assertEventJob(completedEvent, createTableQueryId, createTableQuery);
        assertThat(completedEvent.getInputs())
                .hasSize(4);
        assertCompletedEventInput(
                completedEvent.getInputs().getFirst(),
                "tpch.tiny.supplier",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("address", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("phone", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("suppkey", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(1),
                "tpch.tiny.nation",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(2),
                "tpch.tiny.lineitem",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("orderkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("quantity", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("suppkey", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(3),
                "tpch.tiny.orders",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("orderkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("orderdate", null, null, null, null)));

        assertThat(completedEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedColumnLineage = ImmutableMap.of(
                "suppkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.supplier")
                                .field("suppkey")
                                .build()))
                        .build(),
                "name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.supplier")
                                .field("name")
                                .build()))
                        .build(),
                "address", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.supplier")
                                .field("address")
                                .build()))
                        .build(),
                "phone", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.supplier")
                                .field("phone")
                                .build()))
                        .build(),
                "nation_name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("name")
                                .build()))
                        .build());
        List<InputField> expectedColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.supplier", "address", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.supplier", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.supplier", "phone", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.supplier", "suppkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.supplier", "name", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.lineitem", "orderkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.lineitem", "quantity", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.lineitem", "suppkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.orders", "orderdate", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.orders", "orderkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "name", null));
        List<SchemaDatasetFacetFields> expectedSchemaFields = ImmutableList.of(
                openLineage.newSchemaDatasetFacetFields("suppkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("name", "varchar(25)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("address", "varchar(40)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("phone", "varchar(15)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("nation_name", "varchar(25)", null, null, null));
        assertCompletedEventOutput(
                completedEvent.getOutputs().getFirst(),
                "marquez.default.active_suppliers",
                expectedColumnLineage,
                expectedColumnLineageDataset,
                expectedSchemaFields);
    }

    @Override
    public void assertCreateTableWithUnion(String createTableQueryId, String createTableQuery, String fullTableName, SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(2);

        assertThat(processedEvents.getFirst())
                .isInstanceOf(RunEvent.class);
        RunEvent startEvent = (RunEvent) processedEvents.getFirst();
        assertStartEvent(startEvent, now);
        assertStartEventRun(startEvent, createTableQueryId, "INSERT", session);
        assertEventJob(startEvent, createTableQueryId, createTableQuery);

        assertThat(processedEvents.get(1))
                .isInstanceOf(RunEvent.class);
        RunEvent completedEvent = (RunEvent) processedEvents.get(1);
        assertCompletedEvent(completedEvent, now, 3);
        assertCompletedEventRun(completedEvent, now, createTableQueryId, session);
        assertEventJob(completedEvent, createTableQueryId, createTableQuery);
        assertThat(completedEvent.getInputs())
                .hasSize(3);
        assertCompletedEventInput(
                completedEvent.getInputs().getFirst(),
                "tpch.tiny.orders",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("totalprice", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("orderdate", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(1),
                "tpcds.tiny.store_sales",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("ss_sold_date_sk", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("ss_sales_price", null, null, null, null)));
        assertCompletedEventInput(
                completedEvent.getInputs().get(2),
                "tpcds.tiny.date_dim",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("d_date_sk", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("d_year", null, null, null, null)));

        assertThat(completedEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedColumnLineage = ImmutableMap.of(
                "dataset", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of())
                        .build(),
                "metric_type", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of())
                        .build(),
                "record_count", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of())
                        .build(),
                "total_value", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpch.tiny.orders")
                                        .field("totalprice")
                                        .build(),
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpcds.tiny.store_sales")
                                        .field("ss_sales_price")
                                        .build()))
                        .build(),
                "avg_value", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpch.tiny.orders")
                                        .field("totalprice")
                                        .build(),
                                openLineage.newInputFieldBuilder()
                                        .namespace(OPEN_LINEAGE_NAMESPACE)
                                        .name("tpcds.tiny.store_sales")
                                        .field("ss_sales_price")
                                        .build()))
                        .build());
        List<InputField> expectedColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.orders", "totalprice", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.orders", "orderdate", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.store_sales", "ss_sales_price", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.store_sales", "ss_sold_date_sk", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.date_dim", "d_date_sk", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpcds.sf0.01.date_dim", "d_year", null));
        List<SchemaDatasetFacetFields> expectedSchemaFields = ImmutableList.of(
                openLineage.newSchemaDatasetFacetFields("dataset", "varchar(6)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("metric_type", "varchar(15)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("record_count", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("total_value", "double", null, null, null),
                openLineage.newSchemaDatasetFacetFields("avg_value", "double", null, null, null));
        assertCompletedEventOutput(
                completedEvent.getOutputs().getFirst(),
                fullTableName,
                expectedColumnLineage,
                expectedColumnLineageDataset,
                expectedSchemaFields);
        tearDown();
    }

    @Override
    void assertInsertIntoTable(
            String createTableQueryId,
            String createTableQuery,
            String insertQueryId,
            String insertQuery,
            String fullTableName,
            SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(4);

        assertStartCompleteEventOrder(processedEvents);
        RunEvent startCreateView = getEventOfTypeAtIndex(processedEvents, START, 0);
        assertStartEvent(startCreateView, now);
        assertStartEventRun(startCreateView, createTableQueryId, "INSERT", session);
        assertEventJob(startCreateView, createTableQueryId, createTableQuery);

        RunEvent startCreateTableEvent = getEventOfTypeAtIndex(processedEvents, START, 1);
        assertStartEvent(startCreateTableEvent, now);
        assertStartEventRun(startCreateTableEvent, insertQueryId, "INSERT", session);
        assertEventJob(startCreateTableEvent, insertQueryId, insertQuery);

        RunEvent completeCreateTableEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 0);
        assertCompletedEvent(completeCreateTableEvent, now, 1);
        assertCompletedEventRun(completeCreateTableEvent, now, createTableQueryId, session);
        assertEventJob(completeCreateTableEvent, createTableQueryId, createTableQuery);

        assertThat(completeCreateTableEvent.getInputs())
                .hasSize(1);
        assertCompletedEventInput(
                completeCreateTableEvent.getInputs().getFirst(),
                "tpch.tiny.nation",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("regionkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("comment", null, null, null, null)));

        assertThat(completeCreateTableEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedCreateTableColumnLineage = ImmutableMap.of(
                "nationkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("nationkey")
                                .build()))
                        .build(),
                "name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("name")
                                .build()))
                        .build(),
                "regionkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("regionkey")
                                .build()))
                        .build(),
                "comment", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("comment")
                                .build()))
                        .build());
        List<InputField> expectedCreateTableColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "regionkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "name", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "comment", null));
        List<SchemaDatasetFacetFields> expectedCreateTableSchemaFields = List.of(
                openLineage.newSchemaDatasetFacetFields("nationkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("name", "varchar(25)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("regionkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("comment", "varchar(152)", null, null, null));
        assertCompletedEventOutput(
                completeCreateTableEvent.getOutputs().getFirst(),
                fullTableName,
                expectedCreateTableColumnLineage,
                expectedCreateTableColumnLineageDataset,
                expectedCreateTableSchemaFields);

        RunEvent completeInsertIntoTableEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 1);
        assertCompletedEvent(completeInsertIntoTableEvent, now, 1);
        assertCompletedEventRun(completeInsertIntoTableEvent, now, insertQueryId, session);
        assertEventJob(completeInsertIntoTableEvent, insertQueryId, insertQuery);

        assertThat(completeInsertIntoTableEvent.getInputs())
                .hasSize(1);
        assertCompletedEventInput(
                completeInsertIntoTableEvent.getInputs().getFirst(),
                "tpch.tiny.nation",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("regionkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("comment", null, null, null, null)));

        assertThat(completeInsertIntoTableEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedInsertIntoTableColumnLineage = ImmutableMap.of(
                "nationkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("nationkey")
                                .build()))
                        .build(),
                "name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("name")
                                .build()))
                        .build(),
                "regionkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("regionkey")
                                .build()))
                        .build(),
                "comment", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.nation")
                                .field("comment")
                                .build()))
                        .build());
        List<InputField> expectedInsertIntoTableColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "regionkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "name", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.nation", "comment", null));
        List<SchemaDatasetFacetFields> expectedInsertIntoTableSchemaFields = List.of(
                openLineage.newSchemaDatasetFacetFields("nationkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("name", "varchar(25)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("regionkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("comment", "varchar(152)", null, null, null));
        assertCompletedEventOutput(
                completeInsertIntoTableEvent.getOutputs().getFirst(),
                fullTableName,
                expectedInsertIntoTableColumnLineage,
                expectedInsertIntoTableColumnLineageDataset,
                expectedInsertIntoTableSchemaFields);
    }

    @Override
    void assertDeleteFromTable(
            String createSchemaQueryId,
            String createSchemaQuery,
            String createTableQueryId,
            String createTableQuery,
            String deleteQueryId,
            String deleteQuery,
            String fullTableName,
            SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(6);

        assertStartCompleteEventOrder(processedEvents);
        RunEvent startCreateSchema = getEventOfTypeAtIndex(processedEvents, START, 0);
        assertStartEvent(startCreateSchema, now);
        assertStartEventRun(startCreateSchema, createSchemaQueryId, "DATA_DEFINITION", session);
        assertEventJob(startCreateSchema, createSchemaQueryId, createSchemaQuery);

        RunEvent startCreateTableEvent = getEventOfTypeAtIndex(processedEvents, START, 1);
        assertStartEvent(startCreateTableEvent, now);
        assertStartEventRun(startCreateTableEvent, createTableQueryId, "INSERT", session);
        assertEventJob(startCreateTableEvent, createTableQueryId, createTableQuery);

        RunEvent startDeleteFromTableEvent = getEventOfTypeAtIndex(processedEvents, START, 2);
        assertStartEvent(startDeleteFromTableEvent, now);
        assertStartEventRun(startDeleteFromTableEvent, deleteQueryId, "DELETE", session);
        assertEventJob(startDeleteFromTableEvent, deleteQueryId, deleteQuery);

        RunEvent completeCreateSchemaEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 0);
        assertCompletedEvent(completeCreateSchemaEvent, now, 0, 0);
        assertEventJob(completeCreateSchemaEvent, createSchemaQueryId, createSchemaQuery);

        RunEvent completeCreateTableEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 1);
        assertCompletedEvent(completeCreateTableEvent, now, 1);
        assertCompletedEventRun(completeCreateTableEvent, now, createTableQueryId, session);
        assertEventJob(completeCreateTableEvent, createTableQueryId, createTableQuery);

        assertThat(completeCreateTableEvent.getInputs())
                .hasSize(1);
        assertCompletedEventInput(
                completeCreateTableEvent.getInputs().getFirst(),
                "tpch.tiny.customer",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("mktsegment", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("custkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null)));

        assertThat(completeCreateTableEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedCreateTableColumnLineage = ImmutableMap.of(
                "custkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.customer")
                                .field("custkey")
                                .build()))
                        .build(),
                "name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.customer")
                                .field("name")
                                .build()))
                        .build(),
                "mktsegment", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.customer")
                                .field("mktsegment")
                                .build()))
                        .build(),
                "nationkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.customer")
                                .field("nationkey")
                                .build()))
                        .build());
        List<InputField> expectedCreateTableColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "mktsegment", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "custkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "name", null));
        List<SchemaDatasetFacetFields> expectedCreateTableSchemaFields = List.of(
                openLineage.newSchemaDatasetFacetFields("custkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("name", "varchar(25)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("mktsegment", "varchar(10)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("nationkey", "bigint", null, null, null));
        assertCompletedEventOutput(
                completeCreateTableEvent.getOutputs().getFirst(),
                fullTableName,
                expectedCreateTableColumnLineage,
                expectedCreateTableColumnLineageDataset,
                expectedCreateTableSchemaFields);

        RunEvent completeDeleteFromTableEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 2);
        assertCompletedEvent(completeDeleteFromTableEvent, now, 1);
        assertCompletedEventRun(completeDeleteFromTableEvent, now, deleteQueryId, "DELETE", session);
        assertEventJob(completeDeleteFromTableEvent, deleteQueryId, deleteQuery);

        assertThat(completeDeleteFromTableEvent.getInputs())
                .hasSize(1);
        assertCompletedEventInput(
                completeDeleteFromTableEvent.getInputs().getFirst(),
                "tpch.tiny.customer",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("custkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("acctbal", null, null, null, null)));

        assertThat(completeDeleteFromTableEvent.getOutputs())
                .hasSize(1);
        List<InputField> expectedDeleteFromTableColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "blackhole.schema_delete.customer_backup", "custkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "blackhole.schema_delete.customer_backup", "row_id", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "custkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "acctbal", null));
        assertCompletedEventOutput(
                completeDeleteFromTableEvent.getOutputs().getFirst(),
                fullTableName,
                ImmutableMap.of(),
                expectedDeleteFromTableColumnLineageDataset,
                ImmutableList.of());
    }

    @Override
    void assertMergeIntoTable(
            String createSchemaQueryId,
            String createSchemaQuery,
            String createTableQueryId,
            String createTableQuery,
            String mergeQueryId,
            String mergeQuery,
            String fullTableName,
            SessionRepresentation session)
    {
        Instant now = Instant.now();

        List<BaseEvent> processedEvents = openLineageMemoryTransport.getProcessedEvents();
        assertThat(processedEvents).hasSize(6);

        assertStartCompleteEventOrder(processedEvents);
        RunEvent startCreateSchema = getEventOfTypeAtIndex(processedEvents, START, 0);
        assertStartEvent(startCreateSchema, now);
        assertStartEventRun(startCreateSchema, createSchemaQueryId, "DATA_DEFINITION", session);
        assertEventJob(startCreateSchema, createSchemaQueryId, createSchemaQuery);

        RunEvent startCreateTableEvent = getEventOfTypeAtIndex(processedEvents, START, 1);
        assertStartEvent(startCreateTableEvent, now);
        assertStartEventRun(startCreateTableEvent, createTableQueryId, "INSERT", session);
        assertEventJob(startCreateTableEvent, createTableQueryId, createTableQuery);

        RunEvent startDeleteFromTableEvent = getEventOfTypeAtIndex(processedEvents, START, 2);
        assertStartEvent(startDeleteFromTableEvent, now);
        assertStartEventRun(startDeleteFromTableEvent, mergeQueryId, "MERGE", session);
        assertEventJob(startDeleteFromTableEvent, mergeQueryId, mergeQuery);

        RunEvent completeCreateSchemaEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 0);
        assertCompletedEvent(completeCreateSchemaEvent, now, 0, 0);
        assertEventJob(completeCreateSchemaEvent, createSchemaQueryId, createSchemaQuery);

        RunEvent completeCreateTableEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 1);
        assertCompletedEvent(completeCreateTableEvent, now, 1);
        assertCompletedEventRun(completeCreateTableEvent, now, createTableQueryId, session);
        assertEventJob(completeCreateTableEvent, createTableQueryId, createTableQuery);

        assertThat(completeCreateTableEvent.getInputs())
                .hasSize(1);
        assertCompletedEventInput(
                completeCreateTableEvent.getInputs().getFirst(),
                "tpch.tiny.customer",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("mktsegment", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("custkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null)));

        assertThat(completeCreateTableEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedCreateTableColumnLineage = ImmutableMap.of(
                "custkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.customer")
                                .field("custkey")
                                .build()))
                        .build(),
                "name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.customer")
                                .field("name")
                                .build()))
                        .build(),
                "mktsegment", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.customer")
                                .field("mktsegment")
                                .build()))
                        .build(),
                "nationkey", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of(openLineage.newInputFieldBuilder()
                                .namespace(OPEN_LINEAGE_NAMESPACE)
                                .name("tpch.tiny.customer")
                                .field("nationkey")
                                .build()))
                        .build());
        List<InputField> expectedCreateTableColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "mktsegment", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "custkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "name", null));
        List<SchemaDatasetFacetFields> expectedCreateTableSchemaFields = List.of(
                openLineage.newSchemaDatasetFacetFields("custkey", "bigint", null, null, null),
                openLineage.newSchemaDatasetFacetFields("name", "varchar(25)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("mktsegment", "varchar(10)", null, null, null),
                openLineage.newSchemaDatasetFacetFields("nationkey", "bigint", null, null, null));
        assertCompletedEventOutput(
                completeCreateTableEvent.getOutputs().getFirst(),
                fullTableName,
                expectedCreateTableColumnLineage,
                expectedCreateTableColumnLineageDataset,
                expectedCreateTableSchemaFields);

        RunEvent completeMergeIntoTableEvent = getEventOfTypeAtIndex(processedEvents, COMPLETE, 2);
        assertCompletedEvent(completeMergeIntoTableEvent, now, 1);
        assertCompletedEventRun(completeMergeIntoTableEvent, now, mergeQueryId, "MERGE", session);
        assertEventJob(completeMergeIntoTableEvent, mergeQueryId, mergeQuery);

        assertThat(completeMergeIntoTableEvent.getInputs())
                .hasSize(1);
        assertCompletedEventInput(
                completeMergeIntoTableEvent.getInputs().getFirst(),
                "tpch.tiny.customer",
                ImmutableList.of(
                        openLineage.newSchemaDatasetFacetFields("mktsegment", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("nationkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("custkey", null, null, null, null),
                        openLineage.newSchemaDatasetFacetFields("name", null, null, null, null)));

        assertThat(completeMergeIntoTableEvent.getOutputs())
                .hasSize(1);
        Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedMergeIntoTableColumnLineage = ImmutableMap.of(
                "name", openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(ImmutableList.of())
                        .build());
        List<InputField> expectedMergeIntoTableColumnLineageDataset = ImmutableList.of(
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "blackhole.schema_merge.customer_backup", "mktsegment", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "blackhole.schema_merge.customer_backup", "custkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "blackhole.schema_merge.customer_backup", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "blackhole.schema_merge.customer_backup", "row_id", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "mktsegment", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "custkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "nationkey", null),
                openLineage.newInputField(OPEN_LINEAGE_NAMESPACE, "tpch.tiny.customer", "name", null));
        List<SchemaDatasetFacetFields> expectedMergeIntoTableSchemaFields = List.of(
                openLineage.newSchemaDatasetFacetFields("name", "varchar(25)", null, null, null));
        assertCompletedEventOutput(
                completeMergeIntoTableEvent.getOutputs().getFirst(),
                fullTableName,
                expectedMergeIntoTableColumnLineage,
                expectedMergeIntoTableColumnLineageDataset,
                expectedMergeIntoTableSchemaFields);
    }

    private void assertCompletedEventOutput(
            OutputDataset outputDataset,
            String expectedName,
            Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedColumnLineage,
            List<InputField> expectedDatasetInputFields,
            List<SchemaDatasetFacetFields> expectedSchemaFields)
    {
        assertCompletedEventOutput(
                outputDataset,
                expectedName,
                expectedColumnLineage,
                expectedDatasetInputFields,
                expectedSchemaFields,
                LineageTestTableType.TABLE);
    }

    private void assertCompletedEventOutput(
            OutputDataset outputDataset,
            String expectedName,
            Map<String, ColumnLineageDatasetFacetFieldsAdditional> expectedColumnLineage,
            List<InputField> expectedDatasetInputFields,
            List<SchemaDatasetFacetFields> expectedSchemaFields,
            LineageTestTableType tableType)
    {
        assertThat(outputDataset)
                .satisfies(output -> assertThat(output.getName()).isEqualTo(expectedName))
                .satisfies(output -> assertThat(output.getNamespace()).isEqualTo(OPEN_LINEAGE_NAMESPACE))
                .satisfies(output -> assertThat(output.getFacets().getColumnLineage())
                        .satisfies(columnLineageFacet -> {
                            assertThat(columnLineageFacet.getFields().getAdditionalProperties())
                                    .hasSize(expectedColumnLineage.size());
                            expectedColumnLineage.forEach((columnName, expectedColumnAdditionalFields) ->
                                    assertThat(columnLineageFacet.getFields().getAdditionalProperties())
                                            .hasEntrySatisfying(columnName, columnAdditionalFields -> assertThat(columnAdditionalFields)
                                                    .usingRecursiveComparison()
                                                    .isEqualTo(expectedColumnAdditionalFields)));
                        }))
                .satisfies(output -> assertThat(output.getFacets().getColumnLineage())
                        .satisfies(columnLineageFacet -> {
                            if (tableType.hasColumnLineageDataset()) {
                                assertThat(columnLineageFacet.getDataset())
                                        .hasSize(expectedDatasetInputFields.size());
                                assertThat(columnLineageFacet.getDataset())
                                        .usingRecursiveFieldByFieldElementComparator()
                                        .containsExactlyInAnyOrderElementsOf(expectedDatasetInputFields);
                            }
                        }))
                .satisfies(output -> assertThat(output.getFacets().getSchema())
                        .satisfies(schemaFacet -> assertThat(schemaFacet.getFields())
                                .usingRecursiveFieldByFieldElementComparator()
                                .containsExactlyElementsOf(expectedSchemaFields)))
                .satisfies(output -> assertThat(output.getFacets().getDataSource())
                        .satisfies(dataSourceFacet -> {
                            assertThat(expectedName).startsWith(dataSourceFacet.getName());
                            // URI is missing table name, but it include the namespace/catalog.schema
                            assertThat(format("%s/%s", OPEN_LINEAGE_NAMESPACE, expectedName)).startsWith(dataSourceFacet.getUri().toString());
                        }));
    }

    private void assertCompletedEventInput(InputDataset inputDataset, String expectedName, List<SchemaDatasetFacetFields> expectedDatasetFacetFields)
    {
        assertThat(inputDataset)
                .satisfies(input -> assertThat(input.getName()).isEqualTo(expectedName))
                .satisfies(input -> assertThat(input.getNamespace()).isEqualTo(OPEN_LINEAGE_NAMESPACE))
                .satisfies(input -> assertThat(input.getFacets().getSchema())
                        .satisfies(schemaFacet -> assertThat(schemaFacet.getFields())
                                .usingRecursiveFieldByFieldElementComparator()
                                .containsExactlyElementsOf(expectedDatasetFacetFields)));
    }

    private static void assertCompletedEventRun(RunEvent completedEvent, Instant now, String queryId, SessionRepresentation session)
    {
        assertCompletedEventRun(
                completedEvent,
                now,
                queryId,
                LineageTestTableType.TABLE,
                LineageTestTableType.TABLE.ctasQueryType().name(),
                session);
    }

    private static void assertCompletedEventRun(
            RunEvent completedEvent,
            Instant now,
            String queryId,
            LineageTestTableType tableType,
            SessionRepresentation session)
    {
        assertCompletedEventRun(
                completedEvent,
                now,
                queryId,
                tableType,
                tableType.ctasQueryType().name(),
                session);
    }

    private static void assertCompletedEventRun(RunEvent completedEvent, Instant now, String queryId, String queryType, SessionRepresentation session)
    {
        assertCompletedEventRun(
                completedEvent,
                now,
                queryId,
                LineageTestTableType.TABLE,
                queryType,
                session);
    }

    private static void assertCompletedEventRun(
            RunEvent completedEvent,
            Instant now,
            String queryId,
            LineageTestTableType tableType,
            String queryType,
            SessionRepresentation session)
    {
        assertThat(completedEvent.getRun())
                .satisfies(run -> assertThat(run.getRunId()).isNotNull())
                .satisfies(run -> assertThat(run.getFacets().getProcessing_engine().getName()).isEqualTo("trino"))
                .satisfies(run -> assertThat(run.getFacets().getProcessing_engine().getVersion()).isEqualTo("testversion"))
                .satisfies(run -> assertThat(run.getFacets().getAdditionalProperties())
                        .hasEntrySatisfying("trino_metadata", trinoMetadata -> {
                            assertThat(trinoMetadata.getAdditionalProperties().get("transaction_id")).isNotNull();
                            if (tableType.hasQueryPlanInEvent()) {
                                assertThat(trinoMetadata.getAdditionalProperties().get("query_plan")).isNotNull();
                            }
                            else {
                                assertThat(trinoMetadata.getAdditionalProperties().get("query_plan")).isNull();
                            }
                            assertThat(trinoMetadata.getAdditionalProperties().get("query_id")).isEqualTo(queryId);
                        })
                        .hasEntrySatisfying("trino_query_context", trinoQueryContext -> assertTrinoQueryContext(trinoQueryContext, queryType, session))
                        .hasEntrySatisfying("trino_query_statistics", trinoQueryStatistics ->
                                assertThat(trinoQueryStatistics.getAdditionalProperties()).hasSize(tableType.numberOfStatistics())))
                .satisfies(run -> {
                    assertThat(run.getFacets().getNominalTime()).isNotNull();
                    assertThat(run.getFacets().getNominalTime().getNominalStartTime()).isBefore(now.atZone(ZoneOffset.UTC));
                    assertThat(run.getFacets().getNominalTime().getNominalEndTime()).isAfter(run.getFacets().getNominalTime().getNominalStartTime());
                });
    }

    private static void assertCompletedEvent(RunEvent completedEvent, Instant now, int expectedInputsSize)
    {
        assertCompletedEvent(completedEvent, now, expectedInputsSize, 1);
    }

    private static void assertCompletedEvent(RunEvent completedEvent, Instant now, int expectedInputsSize, int expectedOutputSize)
    {
        assertThat(completedEvent)
                .satisfies(runEvent -> assertThat(runEvent.getEventTime().toInstant()).isBefore(now))
                .satisfies(runEvent -> assertThat(runEvent.getEventType()).isEqualTo(COMPLETE))
                .satisfies(runEvent -> assertThat(runEvent.getProducer()).isNotNull())
                .satisfies(runEvent -> assertThat(runEvent.getSchemaURL()).isNotNull())
                .satisfies(runEvent -> assertThat(runEvent.getRun()).isNotNull())
                .satisfies(runEvent -> assertThat(runEvent.getJob()).isNotNull())
                .satisfies(runEvent -> assertThat(runEvent.getInputs()).hasSize(expectedInputsSize))
                .satisfies(runEvent -> assertThat(runEvent.getOutputs()).hasSize(expectedOutputSize));
    }

    private static void assertStartEvent(RunEvent startEvent, Instant now)
    {
        assertThat(startEvent)
                .satisfies(runEvent -> assertThat(runEvent.getEventTime().toInstant()).isBefore(now))
                .satisfies(runEvent -> assertThat(runEvent.getEventType()).isEqualTo(START))
                .satisfies(runEvent -> assertThat(runEvent.getProducer()).isNotNull())
                .satisfies(runEvent -> assertThat(runEvent.getSchemaURL()).isNotNull())
                .satisfies(runEvent -> assertThat(runEvent.getRun()).isNotNull())
                .satisfies(runEvent -> assertThat(runEvent.getJob()).isNotNull())
                .satisfies(runEvent -> assertThat(runEvent.getInputs()).isNull())
                .satisfies(runEvent -> assertThat(runEvent.getOutputs()).isNull());
    }

    private static void assertEventJob(RunEvent startEvent, String queryId, String query)
    {
        assertThat(startEvent.getJob())
                .satisfies(job -> assertThat(job.getNamespace()).isEqualTo(OPEN_LINEAGE_NAMESPACE))
                .satisfies(job -> assertThat(job.getName()).isEqualTo(queryId))
                .satisfies(job -> assertThat(job.getFacets().getSql()).isNotNull())
                .satisfies(job -> assertThat(job.getFacets().getSql())
                        .satisfies(sqlFacet -> assertThat(sqlFacet.getQuery()).isEqualTo(query)));
    }

    private static void assertStartEventRun(RunEvent startEvent, String queryId, String queryType, SessionRepresentation session)
    {
        assertThat(startEvent.getRun())
                .satisfies(run -> assertThat(run.getRunId()).isNotNull())
                .satisfies(run -> assertThat(run.getFacets().getProcessing_engine().getName()).isEqualTo("trino"))
                .satisfies(run -> assertThat(run.getFacets().getProcessing_engine().getVersion()).isEqualTo("testversion"))
                .satisfies(run -> assertThat(run.getFacets().getAdditionalProperties())
                        .hasEntrySatisfying("trino_metadata", trinoMetadata -> {
                            assertThat(trinoMetadata.getAdditionalProperties().get("transaction_id")).isNotNull();
                            assertThat(trinoMetadata.getAdditionalProperties().get("query_plan")).isNull();
                            assertThat(trinoMetadata.getAdditionalProperties().get("query_id")).isEqualTo(queryId);
                        })
                        .hasEntrySatisfying("trino_query_context", trinoQueryContext -> assertTrinoQueryContext(trinoQueryContext, queryType, session)));
    }

    private static void assertTrinoQueryContext(RunFacet trinoQueryContext, String queryType, SessionRepresentation session)
    {
        assertThat(trinoQueryContext.getAdditionalProperties().get("server_address")).isEqualTo("127.0.0.1");
        assertThat(trinoQueryContext.getAdditionalProperties().get("environment")).isEqualTo("testing");
        assertThat(trinoQueryContext.getAdditionalProperties().get("query_type")).isEqualTo(queryType);
        assertThat(trinoQueryContext.getAdditionalProperties().get("user")).isEqualTo(session.getUser());
        assertThat(trinoQueryContext.getAdditionalProperties().get("original_user")).isEqualTo(session.getOriginalUser());
        assertThat(trinoQueryContext.getAdditionalProperties().get("principal")).isEqualTo(session.getUser());
        assertThat(trinoQueryContext.getAdditionalProperties().get("source")).isEqualTo(session.getSource().orElseThrow());
        assertThat(trinoQueryContext.getAdditionalProperties().get("client_info")).isNull();
        assertThat(trinoQueryContext.getAdditionalProperties().get("remote_client_address")).isEqualTo("127.0.0.1");
        assertThat(trinoQueryContext.getAdditionalProperties().get("user_agent")).isNotNull();
        assertThat(trinoQueryContext.getAdditionalProperties().get("trace_token")).isNull();
    }

    private static RunEvent getEventOfTypeAtIndex(List<BaseEvent> processedEvents, EventType eventType, int skip)
    {
        return processedEvents.stream()
                .map(event -> (RunEvent) event)
                .filter(event -> event.getEventType() == eventType)
                .skip(skip)
                .findFirst()
                .orElseThrow();
    }

    private static void assertStartCompleteEventOrder(List<BaseEvent> processedEvents)
    {
        assertThat(processedEvents)
                .allSatisfy(event -> assertThat(event).isInstanceOf(RunEvent.class))
                .map(event -> (RunEvent) event)
                .extracting(RunEvent::getEventType)
                .is(new Condition<>(
                        TestOpenLineageEventsFromQueries::isValidEventOrder,
                        "Event types should be in appropriate order. START should be before COMPLETE."));
    }

    private static boolean isValidEventOrder(List<? extends EventType> eventTypes)
    {
        int startWithoutComplete = 0;

        for (EventType eventType : eventTypes) {
            if (eventType == START) {
                startWithoutComplete++;
            }
            else if (eventType == COMPLETE) {
                if (startWithoutComplete <= 0) {
                    return false;
                }
                startWithoutComplete--;
            }
        }
        return startWithoutComplete == 0;
    }

    @AfterEach
    public void tearDown()
    {
        openLineageMemoryTransport.clearProcessedEvents();
    }
}
