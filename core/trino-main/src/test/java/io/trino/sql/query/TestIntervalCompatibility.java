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
package io.trino.sql.query;

import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeId;
import io.trino.testing.TestingMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIntervalCompatibility
{
    @Test
    void testLegacyViewLifecycle()
    {
        try (QueryAssertions assertions = new QueryAssertions(testSessionBuilder().setCatalog("mock").setSchema("test").build())) {
            TestingMetadata metadata = new TestingMetadata()
            {
                @Override
                public List<String> listSchemaNames(ConnectorSession session)
                {
                    return List.of("test");
                }

                @Override
                public void refreshView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition)
                {
                    createView(session, viewName, definition, Map.of(), true);
                }
            };
            var runner = assertions.getQueryRunner();
            runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withMetadataWrapper(_ -> metadata)
                    .build()));
            runner.createCatalog("mock", "mock", Map.of());
            SchemaTableName viewName = new SchemaTableName("test", "legacy_interval");
            // Seed the connector with the SQL and bare column metadata written before the upgrade.
            metadata.createView(runner.getDefaultSession().toConnectorSession(), viewName, new ConnectorViewDefinition(
                    "SELECT CAST('340 00:00:00' AS INTERVAL DAY TO SECOND) AS x",
                    Optional.of("mock"),
                    Optional.of("test"),
                    List.of(new ConnectorViewDefinition.ViewColumn("x", TypeId.of("interval day to second"), Optional.empty())),
                    Optional.empty(),
                    Optional.empty(),
                    true,
                    List.of()), Map.of(), false);

            assertThat(assertions.query("SELECT CAST(x AS VARCHAR) FROM legacy_interval"))
                    .matches("VALUES VARCHAR '340 00:00:00.000'");
            runner.execute("ALTER VIEW legacy_interval REFRESH");
            assertThat(assertions.query("SELECT CAST(x AS VARCHAR) FROM legacy_interval"))
                    .matches("VALUES VARCHAR '340 00:00:00.000'");
            assertThat(metadata.getView(runner.getDefaultSession().toConnectorSession(), viewName).orElseThrow().getColumns().getFirst().getType())
                    .isEqualTo(TypeId.of("interval day(9) to second(3)"));

            String createSql = (String) runner.execute("SHOW CREATE VIEW legacy_interval").getOnlyValue();
            assertThat(createSql).contains("INTERVAL DAY(9) TO SECOND(3)");
            runner.execute("DROP VIEW legacy_interval");
            runner.execute(createSql);
            assertThat(assertions.query("SELECT CAST(x AS VARCHAR) FROM legacy_interval"))
                    .matches("VALUES VARCHAR '340 00:00:00.000'");

            runner.execute("CREATE VIEW new_interval AS SELECT CAST('1 00:00:00' AS INTERVAL DAY TO SECOND) AS x");
            assertThat(runner.execute("SHOW CREATE VIEW new_interval").getOnlyValue().toString())
                    .contains("INTERVAL DAY(2) TO SECOND(6)");
        }
    }

    @Test
    void testDescribeIntervalCapability()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            for (boolean supportsIntervals : List.of(false, true)) {
                Session session = assertions.sessionBuilder()
                        .setClientCapabilities(supportsIntervals ? Set.of(ClientCapabilities.PARAMETRIC_INTERVAL.toString()) : Set.of())
                        .addPreparedStatement("interval_query", "SELECT COALESCE(?, CAST(NULL AS INTERVAL DAY TO SECOND)) AS x")
                        .addPreparedStatement("nested_query", "SELECT COALESCE(?, CAST(NULL AS ARRAY(INTERVAL DAY TO SECOND))) AS x")
                        .addPreparedStatement("timestamp_map_query", "SELECT COALESCE(?, CAST(NULL AS MAP(VARCHAR(9), TIMESTAMP(0)))) AS x")
                        .addPreparedStatement("interval_map_query", "SELECT COALESCE(?, CAST(NULL AS MAP(VARCHAR, INTERVAL DAY TO SECOND))) AS x")
                        .addPreparedStatement("row_query", "SELECT COALESCE(?, CAST(NULL AS ROW(\"UP\" ARRAY(INTERVAL DAY TO SECOND), t TIMESTAMP(6)))) AS x")
                        .build();
                String intervalType = supportsIntervals ? "interval day(2) to second(6)" : "interval day to second";
                for (String statement : List.of("interval_query", "nested_query", "timestamp_map_query", "interval_map_query", "row_query")) {
                    String expected = switch (statement) {
                        case "interval_query" -> intervalType;
                        case "nested_query" -> "array(" + intervalType + ")";
                        case "timestamp_map_query" -> "map(varchar(9), timestamp(0))";
                        case "interval_map_query" -> "map(varchar, " + intervalType + ")";
                        case "row_query" -> "row(\"UP\" array(" + intervalType + "), \"t\" timestamp(6))";
                        default -> throw new IllegalArgumentException("Unexpected statement: " + statement);
                    };
                    assertThat(assertions.getQueryRunner().execute(session, "DESCRIBE INPUT " + statement).getMaterializedRows().getFirst().getField(1))
                            .isEqualTo(expected);
                    assertThat(assertions.getQueryRunner().execute(session, "DESCRIBE OUTPUT " + statement).getMaterializedRows().getFirst().getField(4))
                            .isEqualTo(expected);
                }
            }
        }
    }
}
