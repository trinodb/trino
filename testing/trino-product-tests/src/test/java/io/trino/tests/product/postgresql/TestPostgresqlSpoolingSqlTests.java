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
package io.trino.tests.product.postgresql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PostgreSQL SQL tests running against the PostgreSQL + spooling environment.
 * <p>
 * This class inherits all test methods from {@link BasePostgresqlSqlTests}
 * and runs them against {@link PostgresqlSpoolingEnvironment}, which includes
 * Floci for result spooling.
 */
@ProductTest
@RequiresEnvironment(PostgresqlSpoolingEnvironment.class)
@TestGroup.Postgresql
@TestGroup.PostgresqlSpooling
@TestGroup.ProfileSpecificTests
class TestPostgresqlSpoolingSqlTests
        extends BasePostgresqlSqlTests
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testResultsAreSpooled(PostgresqlSpoolingEnvironment env)
            throws Exception
    {
        URI coordinatorUri = URI.create(env.getTrinoJdbcUrl().replace("jdbc:trino:", "http:"));
        try (HttpClient client = HttpClient.newHttpClient()) {
            JsonNode results = send(client, HttpRequest.newBuilder(coordinatorUri.resolve("/v1/statement"))
                    .header("X-Trino-User", "hive")
                    .header("X-Trino-Query-Data-Encoding", "json")
                    .POST(BodyPublishers.ofString("SELECT * FROM postgresql.public.workers_psql")));
            while (!results.has("data")) {
                results = send(client, HttpRequest.newBuilder(URI.create(results.required("nextUri").asText())));
            }
            JsonNode data = results.get("data");
            assertThat(data.path("encoding").asText()).isEqualTo("json");
            assertThat(data.path("segments"))
                    .isNotEmpty()
                    .allSatisfy(segment -> assertThat(segment.path("type").asText()).isEqualTo("spooled"));
        }
        assertThat(env.listSpooledSegments()).isNotEmpty();
    }

    private static JsonNode send(HttpClient client, HttpRequest.Builder request)
            throws IOException, InterruptedException
    {
        HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return OBJECT_MAPPER.readTree(response.body());
    }
}
