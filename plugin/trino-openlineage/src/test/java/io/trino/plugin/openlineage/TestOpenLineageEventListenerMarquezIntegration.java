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

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

final class TestOpenLineageEventListenerMarquezIntegration
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.get(TestOpenLineageEventListenerMarquezIntegration.class);

    private static MarquezServer server;
    private static String marquezURI;
    private static final String trinoURI = "http://trino-integration-test:1337";
    private static final HttpClient client = HttpClient.newHttpClient();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new MarquezServer());
        marquezURI = server.getMarquezUri().toString();

        return OpenLineageListenerQueryRunner.builder()
                .addListenerProperty("openlineage-event-listener.transport.type", "HTTP")
                .addListenerProperty("openlineage-event-listener.transport.url", server.getMarquezUri().toString())
                .addListenerProperty("openlineage-event-listener.trino.uri", trinoURI)
                .build();
    }

    @Test
    void testCreateTableAsSelectFromTable()
            throws Exception
    {
        String outputTable = "test_create_table_as_select_from_table";

        @Language("SQL") String createTableQuery = format(
                "CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation",
                outputTable);

        String queryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        assertEventually(Duration.valueOf("10s"), () -> {
            URI trino = new URI(trinoURI);

            String expectedNamespace = URLEncoder.encode(format("trino://%s:%s", trino.getHost(), trino.getPort()), UTF_8);
            String expectedJobName = URLEncoder.encode(queryId, UTF_8);

            checkJobRegistration(client, expectedNamespace, expectedJobName);
        });
    }

    @Test
    void testCreateTableAsSelectFromView()
            throws Exception
    {
        String viewName = "test_view";
        String outputTable = "test_create_table_as_select_from_view";

        @Language("SQL") String createViewQuery = format(
                "CREATE VIEW %s AS SELECT * FROM tpch.tiny.nation",
                viewName);

        assertQuerySucceeds(createViewQuery);

        @Language("SQL") String createTableQuery = format(
                "CREATE TABLE %s AS SELECT * FROM %s",
                outputTable, viewName);

        String queryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        assertEventually(Duration.valueOf("10s"), () -> {
            URI trino = new URI(trinoURI);

            String expectedNamespace = URLEncoder.encode(format("trino://%s:%s", trino.getHost(), trino.getPort()), UTF_8);
            String expectedJobName = URLEncoder.encode(queryId, UTF_8);

            checkJobRegistration(client, expectedNamespace, expectedJobName);
        });
    }

    private void checkJobRegistration(HttpClient client, String expectedNamespace, String expectedJobName)
            throws URISyntaxException, IOException, InterruptedException
    {
        HttpRequest requestJob = HttpRequest.newBuilder()
                .uri(new URI(marquezURI + "/api/v1/namespaces/" + expectedNamespace + "/jobs/" + expectedJobName))
                .GET()
                .build();

        HttpResponse<String> responseJob = client.send(requestJob, HttpResponse.BodyHandlers.ofString());

        logger.info(responseJob.body());

        assertThat(responseJob.statusCode()).isEqualTo(200);
        assertThat(responseJob.body().toLowerCase(ENGLISH)).contains("complete");
    }
}
