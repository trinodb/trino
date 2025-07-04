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
import io.trino.testing.QueryRunner;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

final class TestOpenLineageEventListenerMarquezIntegration
        extends BaseTestOpenLineageQueries
{
    private static final Logger logger = Logger.get(TestOpenLineageEventListenerMarquezIntegration.class);

    private static MarquezServer server;
    private static String marquezURI;
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
                .addListenerProperty("openlineage-event-listener.trino.uri", TRINO_URI)
                .build();
    }

    @Override
    public void assertCreateTableAsSelectFromTable(String queryId, String query)
    {
        String expectedQueryId = URLEncoder.encode(queryId, UTF_8);

        checkJobRegistration(client, expectedQueryId);
    }

    @Override
    public void assertCreateTableAsSelectFromView(String createViewQueryId, String createViewQuery, String createTableQueryId, String createTableQuery)
    {
        {
            String expectedQueryId = URLEncoder.encode(createViewQueryId, UTF_8);
            checkJobRegistration(client, expectedQueryId);
        }
        {
            String expectedQueryId = URLEncoder.encode(createTableQueryId, UTF_8);
            checkJobRegistration(client, expectedQueryId);
        }
    }

    private void checkJobRegistration(HttpClient client, String expectedQueryId)
    {
        try {
            String encodedNamespace = URLEncoder.encode(OPEN_LINEAGE_NAMESPACE, UTF_8);
            HttpRequest requestJob = HttpRequest.newBuilder()
                    .uri(new URI(marquezURI + "/api/v1/namespaces/" + encodedNamespace + "/jobs/" + expectedQueryId))
                    .GET()
                    .build();

            HttpResponse<String> responseJob = client.send(requestJob, HttpResponse.BodyHandlers.ofString());

            logger.info(responseJob.body());

            assertThat(responseJob.statusCode()).isEqualTo(200);
            assertThat(responseJob.body().toLowerCase(ENGLISH)).contains("complete");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
