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
package io.trino.verifier;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDatabaseEventClient
{
    private static final VerifierQueryEvent FULL_EVENT = new VerifierQueryEvent(
            "suite_full",
            "runid",
            "source",
            "name",
            true,
            "testcatalog",
            "testschema",
            List.of("TEST_SETUP_QUERY_1", "TEST_SETUP_QUERY_2"),
            "TEST_QUERY",
            List.of("TEST_TEARDOWN_QUERY_1", "TEST_TEARDOWN_QUERY_2"),
            List.of("TEST_SETUP_QUERY_ID_1", "TEST_SETUP_QUERY_ID_2"),
            "TEST_QUERY_ID",
            List.of("TEST_TEARDOWN_QUERY_ID_1", "TEST_TEARDOWN_QUERY_ID_2"),
            1.1,
            2.2,
            "controlcatalog",
            "controlschema",
            List.of("CONTROL_SETUP_QUERY_1", "CONTROL_SETUP_QUERY_2"),
            "CONTROL_QUERY",
            List.of("CONTROL_TEARDOWN_QUERY_1", "CONTROL_TEARDOWN_QUERY_2"),
            List.of("CONTROL_SETUP_QUERY_ID_1", "CONTROL_SETUP_QUERY_ID_2"),
            "CONTROL_QUERY_ID",
            List.of("CONTROL_TEARDOWN_QUERY_ID_1", "CONTROL_TEARDOWN_QUERY_ID_2"),
            3.3,
            4.4,
            "error message");

    private static final VerifierQueryEvent MINIMAL_EVENT = new VerifierQueryEvent(
            "suite_minimal",
            null,
            null,
            null,
            false,
            null,
            null,
            List.of(),
            null,
            List.of(),
            List.of(),
            null,
            List.of(),
            null,
            null,
            null,
            null,
            List.of(),
            null,
            List.of(),
            List.of(),
            null,
            List.of(),
            null,
            null,
            null);

    private MySQLContainer<?> mysqlContainer;
    private String mysqlContainerUrl;
    private JsonCodec<List<String>> codec;
    private DatabaseEventClient eventClient;

    @BeforeAll
    public void setup()
    {
        mysqlContainer = new MySQLContainer<>("mysql:8.0.36");
        mysqlContainer.start();
        mysqlContainerUrl = getJdbcUrl(mysqlContainer);
        codec = new JsonCodecFactory().listJsonCodec(String.class);
        VerifierQueryEventDao dao = Jdbi.create(() -> DriverManager.getConnection(mysqlContainerUrl))
                .installPlugin(new SqlObjectPlugin())
                .onDemand(VerifierQueryEventDao.class);
        eventClient = new DatabaseEventClient(dao, codec);
        eventClient.postConstruct();
    }

    private static String getJdbcUrl(MySQLContainer<?> container)
    {
        return format("%s?user=%s&password=%s&useSSL=false&allowPublicKeyRetrieval=true",
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword());
    }

    @AfterAll
    public void teardown()
    {
        if (mysqlContainer != null) {
            mysqlContainer.close();
            mysqlContainer = null;
            mysqlContainerUrl = null;
        }
        codec = null;
        eventClient = null;
    }

    @Test
    public void testFull()
            throws SQLException
    {
        eventClient.post(FULL_EVENT);

        try (Connection connection = DriverManager.getConnection(mysqlContainerUrl)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT * FROM verifier_query_events WHERE suite = 'suite_full'");
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString("suite")).isEqualTo("suite_full");
                    assertThat(resultSet.getString("run_id")).isEqualTo("runid");
                    assertThat(resultSet.getString("source")).isEqualTo("source");
                    assertThat(resultSet.getString("name")).isEqualTo("name");
                    assertThat(resultSet.getBoolean("failed")).isTrue();
                    assertThat(resultSet.getString("test_catalog")).isEqualTo("testcatalog");
                    assertThat(resultSet.getString("test_schema")).isEqualTo("testschema");
                    assertThat(resultSet.getString("test_setup_query_ids_json")).isEqualTo(codec.toJson(FULL_EVENT.getTestSetupQueryIds()));
                    assertThat(resultSet.getString("test_query_id")).isEqualTo("TEST_QUERY_ID");
                    assertThat(resultSet.getString("test_teardown_query_ids_json")).isEqualTo(codec.toJson(FULL_EVENT.getTestTeardownQueryIds()));
                    assertThat(resultSet.getDouble("test_cpu_time_seconds")).isEqualTo(1.1);
                    assertThat(resultSet.getDouble("test_wall_time_seconds")).isEqualTo(2.2);
                    assertThat(resultSet.getString("control_catalog")).isEqualTo("controlcatalog");
                    assertThat(resultSet.getString("control_schema")).isEqualTo("controlschema");
                    assertThat(resultSet.getString("control_setup_query_ids_json")).isEqualTo(codec.toJson(FULL_EVENT.getControlSetupQueryIds()));
                    assertThat(resultSet.getString("control_query_id")).isEqualTo("CONTROL_QUERY_ID");
                    assertThat(resultSet.getString("control_teardown_query_ids_json")).isEqualTo(codec.toJson(FULL_EVENT.getControlTeardownQueryIds()));
                    assertThat(resultSet.getDouble("control_cpu_time_seconds")).isEqualTo(3.3);
                    assertThat(resultSet.getDouble("control_wall_time_seconds")).isEqualTo(4.4);
                    assertThat(resultSet.getString("error_message")).isEqualTo("error message");
                    assertThat(resultSet.next()).isFalse();
                }
            }
        }
    }

    @Test
    public void testMinimal()
            throws SQLException
    {
        eventClient.post(MINIMAL_EVENT);

        try (Connection connection = DriverManager.getConnection(mysqlContainerUrl)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT * FROM verifier_query_events WHERE suite = 'suite_minimal'");
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString("suite")).isEqualTo("suite_minimal");
                    assertThat(resultSet.getString("run_id")).isNull();
                    assertThat(resultSet.getString("source")).isNull();
                    assertThat(resultSet.getString("name")).isNull();
                    assertThat(resultSet.getBoolean("failed")).isFalse();
                    assertThat(resultSet.getString("test_catalog")).isNull();
                    assertThat(resultSet.getString("test_schema")).isNull();
                    assertThat(resultSet.getString("test_setup_query_ids_json")).isNull();
                    assertThat(resultSet.getString("test_query_id")).isNull();
                    assertThat(resultSet.getString("test_teardown_query_ids_json")).isNull();
                    assertThat(resultSet.getObject("test_cpu_time_seconds")).isNull();
                    assertThat(resultSet.getObject("test_wall_time_seconds")).isNull();
                    assertThat(resultSet.getString("control_catalog")).isNull();
                    assertThat(resultSet.getString("control_schema")).isNull();
                    assertThat(resultSet.getString("control_setup_query_ids_json")).isNull();
                    assertThat(resultSet.getString("control_query_id")).isNull();
                    assertThat(resultSet.getString("control_teardown_query_ids_json")).isNull();
                    assertThat(resultSet.getObject("control_cpu_time_seconds")).isNull();
                    assertThat(resultSet.getObject("control_wall_time_seconds")).isNull();
                    assertThat(resultSet.getString("error_message")).isNull();
                    assertThat(resultSet.next()).isFalse();
                }
            }
        }
    }
}
