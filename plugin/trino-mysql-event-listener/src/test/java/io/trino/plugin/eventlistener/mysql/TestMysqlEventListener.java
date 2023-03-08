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
package io.trino.plugin.eventlistener.mysql;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.airlift.json.JsonCodecFactory;
import io.trino.spi.TrinoWarning;
import io.trino.spi.connector.StandardWarningCode;
import io.trino.spi.eventlistener.ColumnDetail;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;
import org.testcontainers.containers.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestMysqlEventListener
{
    private static final QueryMetadata FULL_QUERY_METADATA = new QueryMetadata(
            "full_query",
            Optional.of("transactionId"),
            "query",
            Optional.of("updateType"),
            Optional.of("preparedQuery"),
            "queryState",
            // not stored
            List.of(),
            // not stored
            List.of(),
            URI.create("http://localhost"),
            Optional.of("plan"),
            Optional.of("jsonplan"),
            Optional.of("stageInfo"));

    private static final QueryStatistics FULL_QUERY_STATISTICS = new QueryStatistics(
            ofMillis(101),
            ofMillis(102),
            ofMillis(103),
            ofMillis(104),
            Optional.of(ofMillis(105)),
            Optional.of(ofMillis(106)),
            Optional.of(ofMillis(107)),
            Optional.of(ofMillis(108)),
            Optional.of(ofMillis(109)),
            Optional.of(ofMillis(110)),
            Optional.of(ofMillis(111)),
            Optional.of(ofMillis(112)),
            Optional.of(ofMillis(113)),
            Optional.of(ofMillis(114)),
            Optional.of(ofMillis(115)),
            115L,
            116L,
            117L,
            118L,
            119L,
            1191L,
            1192L,
            120L,
            121L,
            122L,
            123L,
            124L,
            125L,
            126L,
            127L,
            1271L,
            128.0,
            129.0,
            // not stored
            Collections.emptyList(),
            130,
            true,
            // not stored
            Collections.emptyList(),
            // not stored
            Collections.emptyList(),
            // not stored
            List.of("{operator: \"operator1\"}", "{operator: \"operator2\"}"),
            // not stored
            Optional.empty());

    private static final QueryContext FULL_QUERY_CONTEXT = new QueryContext(
            "user",
            Optional.of("principal"),
            Set.of("group1", "group2"),
            Optional.of("traceToken"),
            Optional.of("remoteAddress"),
            Optional.of("userAgent"),
            Optional.of("clientInfo"),
            Set.of("tag1", "tag2", "tag3"),
            // not stored
            Set.of(),
            Optional.of("source"),
            Optional.of("catalog"),
            Optional.of("schema"),
            Optional.of(new ResourceGroupId("resourceGroup")),
            Map.of("property1", "value1", "property2", "value2"),
            // not stored
            new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
            "serverAddress",
            "serverVersion",
            "environment",
            Optional.of(QueryType.SELECT),
            "TASK");

    private static final QueryIOMetadata FULL_QUERY_IO_METADATA = new QueryIOMetadata(
            List.of(
                    new QueryInputMetadata(
                            "catalog1",
                            "schema1",
                            "table1",
                            List.of("column1", "column2"),
                            Optional.of("connectorInfo1"),
                            new Metrics(ImmutableMap.of()),
                            OptionalLong.of(201),
                            OptionalLong.of(202)),
                    new QueryInputMetadata(
                            "catalog2",
                            "schema2",
                            "table2",
                            List.of("column3", "column4"),
                            Optional.of("connectorInfo2"),
                            new Metrics(ImmutableMap.of()),
                            OptionalLong.of(203),
                            OptionalLong.of(204))),
            Optional.of(new QueryOutputMetadata(
                    "catalog3",
                    "schema3",
                    "table3",
                    Optional.of(List.of(
                            new OutputColumnMetadata(
                                    "column5",
                                    "BIGINT",
                                    Set.of(new ColumnDetail(
                                            "catalog4",
                                            "schema4",
                                            "table4",
                                            "column6"))),
                            new OutputColumnMetadata(
                                    "column6",
                                    "VARCHAR",
                                    Set.of()))),
                    Optional.of("outputMetadata"),
                    Optional.of(TRUE))));

    private static final QueryFailureInfo FULL_FAILURE_INFO = new QueryFailureInfo(
            GENERIC_INTERNAL_ERROR.toErrorCode(),
            Optional.of("failureType"),
            Optional.of("failureMessage"),
            Optional.of("failureTask"),
            Optional.of("failureHost"),
            "failureJson");

    private static final QueryCompletedEvent FULL_QUERY_COMPLETED_EVENT = new QueryCompletedEvent(
            FULL_QUERY_METADATA,
            FULL_QUERY_STATISTICS,
            FULL_QUERY_CONTEXT,
            FULL_QUERY_IO_METADATA,
            Optional.of(FULL_FAILURE_INFO),
            List.of(new TrinoWarning(
                    StandardWarningCode.TOO_MANY_STAGES,
                    "too many stages")),
            Instant.now(),
            Instant.now(),
            Instant.now());

    private static final QueryMetadata MINIMAL_QUERY_METADATA = new QueryMetadata(
            "minimal_query",
            Optional.empty(),
            "query",
            Optional.empty(),
            Optional.empty(),
            "queryState",
            // not stored
            List.of(),
            // not stored
            List.of(),
            URI.create("http://localhost"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private static final QueryStatistics MINIMAL_QUERY_STATISTICS = new QueryStatistics(
            ofMillis(101),
            ofMillis(102),
            ofMillis(103),
            ofMillis(104),
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
            115L,
            116L,
            117L,
            118L,
            119L,
            1191L,
            1192L,
            120L,
            121L,
            122L,
            123L,
            124L,
            125L,
            126L,
            127L,
            1271L,
            128.0,
            129.0,
            // not stored
            Collections.emptyList(),
            130,
            false,
            // not stored
            Collections.emptyList(),
            // not stored
            Collections.emptyList(),
            Collections.emptyList(),
            // not stored
            Optional.empty());

    private static final QueryContext MINIMAL_QUERY_CONTEXT = new QueryContext(
            "user",
            Optional.empty(),
            Set.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Set.of(),
            // not stored
            Set.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of(),
            // not stored
            new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
            "serverAddress",
            "serverVersion",
            "environment",
            Optional.empty(),
            "NONE");

    private static final QueryIOMetadata MINIMAL_QUERY_IO_METADATA = new QueryIOMetadata(List.of(), Optional.empty());

    private static final QueryCompletedEvent MINIMAL_QUERY_COMPLETED_EVENT = new QueryCompletedEvent(
            MINIMAL_QUERY_METADATA,
            MINIMAL_QUERY_STATISTICS,
            MINIMAL_QUERY_CONTEXT,
            MINIMAL_QUERY_IO_METADATA,
            Optional.empty(),
            List.of(),
            Instant.now(),
            Instant.now(),
            Instant.now());

    private MySQLContainer<?> mysqlContainer;
    private String mysqlContainerUrl;
    private EventListener eventListener;
    private JsonCodecFactory jsonCodecFactory;

    @BeforeClass
    public void setup()
    {
        mysqlContainer = new MySQLContainer<>("mysql:8.0.12");
        mysqlContainer.start();
        mysqlContainerUrl = getJdbcUrl(mysqlContainer);
        eventListener = new MysqlEventListenerFactory()
                .create(Map.of("mysql-event-listener.db.url", mysqlContainerUrl));
        jsonCodecFactory = new JsonCodecFactory();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        if (mysqlContainer != null) {
            mysqlContainer.close();
            mysqlContainer = null;
            mysqlContainerUrl = null;
        }
        eventListener = null;
        jsonCodecFactory = null;
    }

    private static String getJdbcUrl(MySQLContainer<?> container)
    {
        return format("%s?user=%s&password=%s&useSSL=false&allowPublicKeyRetrieval=true",
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword());
    }

    @Test
    public void testFull()
            throws SQLException
    {
        eventListener.queryCompleted(FULL_QUERY_COMPLETED_EVENT);

        try (Connection connection = DriverManager.getConnection(mysqlContainerUrl)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT * FROM trino_queries WHERE query_id = 'full_query'");
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertTrue(resultSet.next());
                    assertEquals(resultSet.getString("query_id"), "full_query");
                    assertEquals(resultSet.getString("transaction_id"), "transactionId");
                    assertEquals(resultSet.getString("query"), "query");
                    assertEquals(resultSet.getString("update_type"), "updateType");
                    assertEquals(resultSet.getString("prepared_query"), "preparedQuery");
                    assertEquals(resultSet.getString("query_state"), "queryState");
                    assertEquals(resultSet.getString("plan"), "plan");
                    assertEquals(resultSet.getString("stage_info_json"), "stageInfo");
                    assertEquals(resultSet.getString("user"), "user");
                    assertEquals(resultSet.getString("principal"), "principal");
                    assertEquals(resultSet.getString("trace_token"), "traceToken");
                    assertEquals(resultSet.getString("remote_client_address"), "remoteAddress");
                    assertEquals(resultSet.getString("user_agent"), "userAgent");
                    assertEquals(resultSet.getString("client_info"), "clientInfo");
                    assertEquals(resultSet.getString("client_tags_json"), jsonCodecFactory.jsonCodec(new TypeToken<Set<String>>() {}).toJson(FULL_QUERY_CONTEXT.getClientTags()));
                    assertEquals(resultSet.getString("source"), "source");
                    assertEquals(resultSet.getString("catalog"), "catalog");
                    assertEquals(resultSet.getString("schema"), "schema");
                    assertEquals(resultSet.getString("resource_group_id"), "resourceGroup");
                    assertEquals(resultSet.getString("session_properties_json"), jsonCodecFactory.mapJsonCodec(String.class, String.class).toJson(FULL_QUERY_CONTEXT.getSessionProperties()));
                    assertEquals(resultSet.getString("server_address"), "serverAddress");
                    assertEquals(resultSet.getString("server_version"), "serverVersion");
                    assertEquals(resultSet.getString("environment"), "environment");
                    assertEquals(resultSet.getString("query_type"), "SELECT");
                    assertEquals(resultSet.getString("inputs_json"), jsonCodecFactory.listJsonCodec(QueryInputMetadata.class).toJson(FULL_QUERY_IO_METADATA.getInputs()));
                    assertEquals(resultSet.getString("output_json"), jsonCodecFactory.jsonCodec(QueryOutputMetadata.class).toJson(FULL_QUERY_IO_METADATA.getOutput().orElseThrow()));
                    assertEquals(resultSet.getString("error_code"), GENERIC_INTERNAL_ERROR.name());
                    assertEquals(resultSet.getString("error_type"), GENERIC_INTERNAL_ERROR.toErrorCode().getType().name());
                    assertEquals(resultSet.getString("failure_type"), "failureType");
                    assertEquals(resultSet.getString("failure_message"), "failureMessage");
                    assertEquals(resultSet.getString("failure_task"), "failureTask");
                    assertEquals(resultSet.getString("failure_host"), "failureHost");
                    assertEquals(resultSet.getString("failures_json"), "failureJson");
                    assertEquals(resultSet.getString("warnings_json"), jsonCodecFactory.listJsonCodec(TrinoWarning.class).toJson(FULL_QUERY_COMPLETED_EVENT.getWarnings()));
                    assertEquals(resultSet.getLong("cpu_time_millis"), 101);
                    assertEquals(resultSet.getLong("failed_cpu_time_millis"), 102);
                    assertEquals(resultSet.getLong("wall_time_millis"), 103);
                    assertEquals(resultSet.getLong("queued_time_millis"), 104);
                    assertEquals(resultSet.getLong("scheduled_time_millis"), 105);
                    assertEquals(resultSet.getLong("failed_scheduled_time_millis"), 106);
                    assertEquals(resultSet.getLong("waiting_time_millis"), 107);
                    assertEquals(resultSet.getLong("analysis_time_millis"), 108);
                    assertEquals(resultSet.getLong("planning_time_millis"), 109);
                    assertEquals(resultSet.getLong("execution_time_millis"), 110);
                    assertEquals(resultSet.getLong("input_blocked_time_millis"), 111);
                    assertEquals(resultSet.getLong("failed_input_blocked_time_millis"), 112);
                    assertEquals(resultSet.getLong("output_blocked_time_millis"), 113);
                    assertEquals(resultSet.getLong("failed_output_blocked_time_millis"), 114);
                    assertEquals(resultSet.getLong("physical_input_read_time_millis"), 115);
                    assertEquals(resultSet.getLong("peak_memory_bytes"), 115);
                    assertEquals(resultSet.getLong("peak_task_memory_bytes"), 117);
                    assertEquals(resultSet.getLong("physical_input_bytes"), 118);
                    assertEquals(resultSet.getLong("physical_input_rows"), 119);
                    assertEquals(resultSet.getLong("internal_network_bytes"), 120);
                    assertEquals(resultSet.getLong("internal_network_rows"), 121);
                    assertEquals(resultSet.getLong("total_bytes"), 122);
                    assertEquals(resultSet.getLong("total_rows"), 123);
                    assertEquals(resultSet.getLong("output_bytes"), 124);
                    assertEquals(resultSet.getLong("output_rows"), 125);
                    assertEquals(resultSet.getLong("written_bytes"), 126);
                    assertEquals(resultSet.getLong("written_rows"), 127);
                    assertEquals(resultSet.getDouble("cumulative_memory"), 128.0);
                    assertEquals(resultSet.getDouble("failed_cumulative_memory"), 129.0);
                    assertEquals(resultSet.getLong("completed_splits"), 130);
                    assertEquals(resultSet.getString("retry_policy"), "TASK");
                    assertEquals(resultSet.getString("operator_summaries_json"), "[{operator: \"operator1\"},{operator: \"operator2\"}]");
                    assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    public void testMinimal()
            throws SQLException
    {
        eventListener.queryCompleted(MINIMAL_QUERY_COMPLETED_EVENT);

        try (Connection connection = DriverManager.getConnection(mysqlContainerUrl)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT * FROM trino_queries WHERE query_id = 'minimal_query'");
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertTrue(resultSet.next());
                    assertEquals(resultSet.getString("query_id"), "minimal_query");
                    assertNull(resultSet.getString("transaction_id"));
                    assertEquals(resultSet.getString("query"), "query");
                    assertNull(resultSet.getString("update_type"));
                    assertNull(resultSet.getString("prepared_query"));
                    assertEquals(resultSet.getString("query_state"), "queryState");
                    assertNull(resultSet.getString("plan"));
                    assertNull(resultSet.getString("stage_info_json"));
                    assertEquals(resultSet.getString("user"), "user");
                    assertNull(resultSet.getString("principal"));
                    assertNull(resultSet.getString("trace_token"));
                    assertNull(resultSet.getString("remote_client_address"));
                    assertNull(resultSet.getString("user_agent"));
                    assertNull(resultSet.getString("client_info"));
                    assertEquals(resultSet.getString("client_tags_json"), jsonCodecFactory.jsonCodec(new TypeToken<Set<String>>() {}).toJson(Set.of()));
                    assertNull(resultSet.getString("source"));
                    assertNull(resultSet.getString("catalog"));
                    assertNull(resultSet.getString("schema"));
                    assertNull(resultSet.getString("resource_group_id"));
                    assertEquals(resultSet.getString("session_properties_json"), jsonCodecFactory.mapJsonCodec(String.class, String.class).toJson(Map.of()));
                    assertEquals(resultSet.getString("server_address"), "serverAddress");
                    assertEquals(resultSet.getString("server_version"), "serverVersion");
                    assertEquals(resultSet.getString("environment"), "environment");
                    assertNull(resultSet.getString("query_type"));
                    assertEquals(resultSet.getString("inputs_json"), jsonCodecFactory.listJsonCodec(QueryInputMetadata.class).toJson(List.of()));
                    assertNull(resultSet.getString("output_json"));
                    assertNull(resultSet.getString("error_code"));
                    assertNull(resultSet.getString("error_type"));
                    assertNull(resultSet.getString("failure_type"));
                    assertNull(resultSet.getString("failure_message"));
                    assertNull(resultSet.getString("failure_task"));
                    assertNull(resultSet.getString("failure_host"));
                    assertNull(resultSet.getString("failures_json"));
                    assertEquals(resultSet.getString("warnings_json"), jsonCodecFactory.listJsonCodec(TrinoWarning.class).toJson(List.of()));
                    assertEquals(resultSet.getLong("cpu_time_millis"), 101);
                    assertEquals(resultSet.getLong("failed_cpu_time_millis"), 102);
                    assertEquals(resultSet.getLong("wall_time_millis"), 103);
                    assertEquals(resultSet.getLong("queued_time_millis"), 104);
                    assertEquals(resultSet.getLong("scheduled_time_millis"), 0);
                    assertEquals(resultSet.getLong("failed_scheduled_time_millis"), 0);
                    assertEquals(resultSet.getLong("waiting_time_millis"), 0);
                    assertEquals(resultSet.getLong("analysis_time_millis"), 0);
                    assertEquals(resultSet.getLong("planning_time_millis"), 0);
                    assertEquals(resultSet.getLong("execution_time_millis"), 0);
                    assertEquals(resultSet.getLong("input_blocked_time_millis"), 0);
                    assertEquals(resultSet.getLong("failed_input_blocked_time_millis"), 0);
                    assertEquals(resultSet.getLong("output_blocked_time_millis"), 0);
                    assertEquals(resultSet.getLong("failed_output_blocked_time_millis"), 0);
                    assertEquals(resultSet.getLong("physical_input_read_time_millis"), 0);
                    assertEquals(resultSet.getLong("peak_memory_bytes"), 115);
                    assertEquals(resultSet.getLong("peak_task_memory_bytes"), 117);
                    assertEquals(resultSet.getLong("physical_input_bytes"), 118);
                    assertEquals(resultSet.getLong("physical_input_rows"), 119);
                    assertEquals(resultSet.getLong("internal_network_bytes"), 120);
                    assertEquals(resultSet.getLong("internal_network_rows"), 121);
                    assertEquals(resultSet.getLong("total_bytes"), 122);
                    assertEquals(resultSet.getLong("total_rows"), 123);
                    assertEquals(resultSet.getLong("output_bytes"), 124);
                    assertEquals(resultSet.getLong("output_rows"), 125);
                    assertEquals(resultSet.getLong("written_bytes"), 126);
                    assertEquals(resultSet.getLong("written_rows"), 127);
                    assertEquals(resultSet.getDouble("cumulative_memory"), 128.0);
                    assertEquals(resultSet.getDouble("failed_cumulative_memory"), 129.0);
                    assertEquals(resultSet.getLong("completed_splits"), 130);
                    assertEquals(resultSet.getString("retry_policy"), "NONE");
                    assertEquals(resultSet.getString("operator_summaries_json"), "[]");
                    assertFalse(resultSet.next());
                }
            }
        }
    }
}
