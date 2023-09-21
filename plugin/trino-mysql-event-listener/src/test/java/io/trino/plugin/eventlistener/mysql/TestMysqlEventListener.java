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
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MySQLContainer;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
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
            Optional.of(ofMillis(1091)),
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
            Collections.emptyList(),
            // not stored
            Optional.empty());

    private static final QueryContext FULL_QUERY_CONTEXT = new QueryContext(
            "user",
            "originalUser",
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
                            new CatalogVersion("default"),
                            "schema1",
                            "table1",
                            List.of("column1", "column2"),
                            Optional.of("connectorInfo1"),
                            new Metrics(ImmutableMap.of()),
                            OptionalLong.of(201),
                            OptionalLong.of(202)),
                    new QueryInputMetadata(
                            "catalog2",
                            new CatalogVersion("default"),
                            "schema2",
                            "table2",
                            List.of("column3", "column4"),
                            Optional.of("connectorInfo2"),
                            new Metrics(ImmutableMap.of()),
                            OptionalLong.of(203),
                            OptionalLong.of(204))),
            Optional.of(new QueryOutputMetadata(
                    "catalog3",
                    new CatalogVersion("default"),
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
            Collections.emptyList(),
            // not stored
            Optional.empty());

    private static final QueryContext MINIMAL_QUERY_CONTEXT = new QueryContext(
            "user",
            "originalUser",
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

    @BeforeAll
    public void setup()
    {
        mysqlContainer = new MySQLContainer<>("mysql:8.0.12");
        mysqlContainer.start();
        mysqlContainerUrl = getJdbcUrl(mysqlContainer);
        eventListener = new MysqlEventListenerFactory()
                .create(Map.of("mysql-event-listener.db.url", mysqlContainerUrl));
        jsonCodecFactory = new JsonCodecFactory();
    }

    @AfterAll
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
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString("query_id")).isEqualTo("full_query");
                    assertThat(resultSet.getString("transaction_id")).isEqualTo("transactionId");
                    assertThat(resultSet.getString("query")).isEqualTo("query");
                    assertThat(resultSet.getString("update_type")).isEqualTo("updateType");
                    assertThat(resultSet.getString("prepared_query")).isEqualTo("preparedQuery");
                    assertThat(resultSet.getString("query_state")).isEqualTo("queryState");
                    assertThat(resultSet.getString("plan")).isEqualTo("plan");
                    assertThat(resultSet.getString("stage_info_json")).isEqualTo("stageInfo");
                    assertThat(resultSet.getString("user")).isEqualTo("user");
                    assertThat(resultSet.getString("principal")).isEqualTo("principal");
                    assertThat(resultSet.getString("trace_token")).isEqualTo("traceToken");
                    assertThat(resultSet.getString("remote_client_address")).isEqualTo("remoteAddress");
                    assertThat(resultSet.getString("user_agent")).isEqualTo("userAgent");
                    assertThat(resultSet.getString("client_info")).isEqualTo("clientInfo");
                    assertThat(resultSet.getString("client_tags_json")).isEqualTo(jsonCodecFactory.jsonCodec(new TypeToken<Set<String>>() { }).toJson(FULL_QUERY_CONTEXT.getClientTags()));
                    assertThat(resultSet.getString("source")).isEqualTo("source");
                    assertThat(resultSet.getString("catalog")).isEqualTo("catalog");
                    assertThat(resultSet.getString("schema")).isEqualTo("schema");
                    assertThat(resultSet.getString("resource_group_id")).isEqualTo("resourceGroup");
                    assertThat(resultSet.getString("session_properties_json")).isEqualTo(jsonCodecFactory.mapJsonCodec(String.class, String.class).toJson(FULL_QUERY_CONTEXT.getSessionProperties()));
                    assertThat(resultSet.getString("server_address")).isEqualTo("serverAddress");
                    assertThat(resultSet.getString("server_version")).isEqualTo("serverVersion");
                    assertThat(resultSet.getString("environment")).isEqualTo("environment");
                    assertThat(resultSet.getString("query_type")).isEqualTo("SELECT");
                    assertThat(resultSet.getString("inputs_json")).isEqualTo(jsonCodecFactory.listJsonCodec(QueryInputMetadata.class).toJson(FULL_QUERY_IO_METADATA.getInputs()));
                    assertThat(resultSet.getString("output_json")).isEqualTo(jsonCodecFactory.jsonCodec(QueryOutputMetadata.class).toJson(FULL_QUERY_IO_METADATA.getOutput().orElseThrow()));
                    assertThat(resultSet.getString("error_code")).isEqualTo(GENERIC_INTERNAL_ERROR.name());
                    assertThat(resultSet.getString("error_type")).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode().getType().name());
                    assertThat(resultSet.getString("failure_type")).isEqualTo("failureType");
                    assertThat(resultSet.getString("failure_message")).isEqualTo("failureMessage");
                    assertThat(resultSet.getString("failure_task")).isEqualTo("failureTask");
                    assertThat(resultSet.getString("failure_host")).isEqualTo("failureHost");
                    assertThat(resultSet.getString("failures_json")).isEqualTo("failureJson");
                    assertThat(resultSet.getString("warnings_json")).isEqualTo(jsonCodecFactory.listJsonCodec(TrinoWarning.class).toJson(FULL_QUERY_COMPLETED_EVENT.getWarnings()));
                    assertThat(resultSet.getLong("cpu_time_millis")).isEqualTo(101);
                    assertThat(resultSet.getLong("failed_cpu_time_millis")).isEqualTo(102);
                    assertThat(resultSet.getLong("wall_time_millis")).isEqualTo(103);
                    assertThat(resultSet.getLong("queued_time_millis")).isEqualTo(104);
                    assertThat(resultSet.getLong("scheduled_time_millis")).isEqualTo(105);
                    assertThat(resultSet.getLong("failed_scheduled_time_millis")).isEqualTo(106);
                    assertThat(resultSet.getLong("waiting_time_millis")).isEqualTo(107);
                    assertThat(resultSet.getLong("analysis_time_millis")).isEqualTo(108);
                    assertThat(resultSet.getLong("planning_time_millis")).isEqualTo(109);
                    assertThat(resultSet.getLong("planning_cpu_time_millis")).isEqualTo(1091);
                    assertThat(resultSet.getLong("execution_time_millis")).isEqualTo(110);
                    assertThat(resultSet.getLong("input_blocked_time_millis")).isEqualTo(111);
                    assertThat(resultSet.getLong("failed_input_blocked_time_millis")).isEqualTo(112);
                    assertThat(resultSet.getLong("output_blocked_time_millis")).isEqualTo(113);
                    assertThat(resultSet.getLong("failed_output_blocked_time_millis")).isEqualTo(114);
                    assertThat(resultSet.getLong("physical_input_read_time_millis")).isEqualTo(115);
                    assertThat(resultSet.getLong("peak_memory_bytes")).isEqualTo(115);
                    assertThat(resultSet.getLong("peak_task_memory_bytes")).isEqualTo(117);
                    assertThat(resultSet.getLong("physical_input_bytes")).isEqualTo(118);
                    assertThat(resultSet.getLong("physical_input_rows")).isEqualTo(119);
                    assertThat(resultSet.getLong("internal_network_bytes")).isEqualTo(120);
                    assertThat(resultSet.getLong("internal_network_rows")).isEqualTo(121);
                    assertThat(resultSet.getLong("total_bytes")).isEqualTo(122);
                    assertThat(resultSet.getLong("total_rows")).isEqualTo(123);
                    assertThat(resultSet.getLong("output_bytes")).isEqualTo(124);
                    assertThat(resultSet.getLong("output_rows")).isEqualTo(125);
                    assertThat(resultSet.getLong("written_bytes")).isEqualTo(126);
                    assertThat(resultSet.getLong("written_rows")).isEqualTo(127);
                    assertThat(resultSet.getDouble("cumulative_memory")).isEqualTo(128.0);
                    assertThat(resultSet.getDouble("failed_cumulative_memory")).isEqualTo(129.0);
                    assertThat(resultSet.getLong("completed_splits")).isEqualTo(130);
                    assertThat(resultSet.getString("retry_policy")).isEqualTo("TASK");
                    assertThat(resultSet.getString("operator_summaries_json")).isEqualTo("[{operator: \"operator1\"},{operator: \"operator2\"}]");
                    assertThat(resultSet.next()).isFalse();
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
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString("query_id")).isEqualTo("minimal_query");
                    assertThat(resultSet.getString("transaction_id")).isNull();
                    assertThat(resultSet.getString("query")).isEqualTo("query");
                    assertThat(resultSet.getString("update_type")).isNull();
                    assertThat(resultSet.getString("prepared_query")).isNull();
                    assertThat(resultSet.getString("query_state")).isEqualTo("queryState");
                    assertThat(resultSet.getString("plan")).isNull();
                    assertThat(resultSet.getString("stage_info_json")).isNull();
                    assertThat(resultSet.getString("user")).isEqualTo("user");
                    assertThat(resultSet.getString("principal")).isNull();
                    assertThat(resultSet.getString("trace_token")).isNull();
                    assertThat(resultSet.getString("remote_client_address")).isNull();
                    assertThat(resultSet.getString("user_agent")).isNull();
                    assertThat(resultSet.getString("client_info")).isNull();
                    assertThat(resultSet.getString("client_tags_json")).isEqualTo(jsonCodecFactory.jsonCodec(new TypeToken<Set<String>>() { }).toJson(Set.of()));
                    assertThat(resultSet.getString("source")).isNull();
                    assertThat(resultSet.getString("catalog")).isNull();
                    assertThat(resultSet.getString("schema")).isNull();
                    assertThat(resultSet.getString("resource_group_id")).isNull();
                    assertThat(resultSet.getString("session_properties_json")).isEqualTo(jsonCodecFactory.mapJsonCodec(String.class, String.class).toJson(Map.of()));
                    assertThat(resultSet.getString("server_address")).isEqualTo("serverAddress");
                    assertThat(resultSet.getString("server_version")).isEqualTo("serverVersion");
                    assertThat(resultSet.getString("environment")).isEqualTo("environment");
                    assertThat(resultSet.getString("query_type")).isNull();
                    assertThat(resultSet.getString("inputs_json")).isEqualTo(jsonCodecFactory.listJsonCodec(QueryInputMetadata.class).toJson(List.of()));
                    assertThat(resultSet.getString("output_json")).isNull();
                    assertThat(resultSet.getString("error_code")).isNull();
                    assertThat(resultSet.getString("error_type")).isNull();
                    assertThat(resultSet.getString("failure_type")).isNull();
                    assertThat(resultSet.getString("failure_message")).isNull();
                    assertThat(resultSet.getString("failure_task")).isNull();
                    assertThat(resultSet.getString("failure_host")).isNull();
                    assertThat(resultSet.getString("failures_json")).isNull();
                    assertThat(resultSet.getString("warnings_json")).isEqualTo(jsonCodecFactory.listJsonCodec(TrinoWarning.class).toJson(List.of()));
                    assertThat(resultSet.getLong("cpu_time_millis")).isEqualTo(101);
                    assertThat(resultSet.getLong("failed_cpu_time_millis")).isEqualTo(102);
                    assertThat(resultSet.getLong("wall_time_millis")).isEqualTo(103);
                    assertThat(resultSet.getLong("queued_time_millis")).isEqualTo(104);
                    assertThat(resultSet.getLong("scheduled_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("failed_scheduled_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("waiting_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("analysis_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("planning_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("execution_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("input_blocked_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("failed_input_blocked_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("output_blocked_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("failed_output_blocked_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("physical_input_read_time_millis")).isEqualTo(0);
                    assertThat(resultSet.getLong("peak_memory_bytes")).isEqualTo(115);
                    assertThat(resultSet.getLong("peak_task_memory_bytes")).isEqualTo(117);
                    assertThat(resultSet.getLong("physical_input_bytes")).isEqualTo(118);
                    assertThat(resultSet.getLong("physical_input_rows")).isEqualTo(119);
                    assertThat(resultSet.getLong("internal_network_bytes")).isEqualTo(120);
                    assertThat(resultSet.getLong("internal_network_rows")).isEqualTo(121);
                    assertThat(resultSet.getLong("total_bytes")).isEqualTo(122);
                    assertThat(resultSet.getLong("total_rows")).isEqualTo(123);
                    assertThat(resultSet.getLong("output_bytes")).isEqualTo(124);
                    assertThat(resultSet.getLong("output_rows")).isEqualTo(125);
                    assertThat(resultSet.getLong("written_bytes")).isEqualTo(126);
                    assertThat(resultSet.getLong("written_rows")).isEqualTo(127);
                    assertThat(resultSet.getDouble("cumulative_memory")).isEqualTo(128.0);
                    assertThat(resultSet.getDouble("failed_cumulative_memory")).isEqualTo(129.0);
                    assertThat(resultSet.getLong("completed_splits")).isEqualTo(130);
                    assertThat(resultSet.getString("retry_policy")).isEqualTo("NONE");
                    assertThat(resultSet.getString("operator_summaries_json")).isEqualTo("[]");
                    assertThat(resultSet.next()).isFalse();
                }
            }
        }
    }
}
