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
import org.testcontainers.containers.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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

    @BeforeClass
    public void setup()
    {
        mysqlContainer = new MySQLContainer<>("mysql:8.0.12");
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

    @AfterClass(alwaysRun = true)
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
                    assertTrue(resultSet.next());
                    assertEquals(resultSet.getString("suite"), "suite_full");
                    assertEquals(resultSet.getString("run_id"), "runid");
                    assertEquals(resultSet.getString("source"), "source");
                    assertEquals(resultSet.getString("name"), "name");
                    assertTrue(resultSet.getBoolean("failed"));
                    assertEquals(resultSet.getString("test_catalog"), "testcatalog");
                    assertEquals(resultSet.getString("test_schema"), "testschema");
                    assertEquals(resultSet.getString("test_setup_query_ids_json"), codec.toJson(FULL_EVENT.getTestSetupQueryIds()));
                    assertEquals(resultSet.getString("test_query_id"), "TEST_QUERY_ID");
                    assertEquals(resultSet.getString("test_teardown_query_ids_json"), codec.toJson(FULL_EVENT.getTestTeardownQueryIds()));
                    assertEquals(resultSet.getString("control_catalog"), "controlcatalog");
                    assertEquals(resultSet.getString("control_schema"), "controlschema");
                    assertEquals(resultSet.getString("control_setup_query_ids_json"), codec.toJson(FULL_EVENT.getControlSetupQueryIds()));
                    assertEquals(resultSet.getString("control_query_id"), "CONTROL_QUERY_ID");
                    assertEquals(resultSet.getString("control_teardown_query_ids_json"), codec.toJson(FULL_EVENT.getControlTeardownQueryIds()));
                    assertEquals(resultSet.getString("error_message"), "error message");
                    assertFalse(resultSet.next());
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
                    assertTrue(resultSet.next());
                    assertEquals(resultSet.getString("suite"), "suite_minimal");
                    assertNull(resultSet.getString("run_id"));
                    assertNull(resultSet.getString("source"));
                    assertNull(resultSet.getString("name"));
                    assertFalse(resultSet.getBoolean("failed"));
                    assertNull(resultSet.getString("test_catalog"));
                    assertNull(resultSet.getString("test_schema"));
                    assertNull(resultSet.getString("test_setup_query_ids_json"));
                    assertNull(resultSet.getString("test_query_id"));
                    assertNull(resultSet.getString("test_teardown_query_ids_json"));
                    assertNull(resultSet.getString("control_catalog"));
                    assertNull(resultSet.getString("control_schema"));
                    assertNull(resultSet.getString("control_setup_query_ids_json"));
                    assertNull(resultSet.getString("control_query_id"));
                    assertNull(resultSet.getString("control_teardown_query_ids_json"));
                    assertNull(resultSet.getString("error_message"));
                    assertFalse(resultSet.next());
                }
            }
        }
    }
}
