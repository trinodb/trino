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
import io.trino.SessionRepresentation;
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
    public void assertCreateTableAsSelectFromTable(String queryId, String query, String fullTableName, LineageTestTableType tableType, SessionRepresentation session)
    {
        String expectedJobName = URLEncoder.encode(queryId, UTF_8);

        checkJobRegistration(client, expectedJobName);
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
        {
            String expectedJobName = URLEncoder.encode(createViewQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
        {
            String expectedJobName = URLEncoder.encode(createTableQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
    }

    @Override
    public void assertCreateTableWithJoin(String createTableQueryId, String createTableQuery, SessionRepresentation session)
    {
        String expectedJobName = URLEncoder.encode(createTableQueryId, UTF_8);

        checkJobRegistration(client, expectedJobName);
    }

    @Override
    public void assertCreateTableWithCTE(String createTableQueryId, String createTableQuery, SessionRepresentation session)
    {
        String expectedJobName = URLEncoder.encode(createTableQueryId, UTF_8);

        checkJobRegistration(client, expectedJobName);
    }

    @Override
    public void assertCreateTableWithSubquery(String createTableQueryId, String createTableQuery, SessionRepresentation session)
    {
        String expectedJobName = URLEncoder.encode(createTableQueryId, UTF_8);

        checkJobRegistration(client, expectedJobName);
    }

    @Override
    public void assertCreateTableWithUnion(String createTableQueryId, String createTableQuery, String fullTableName, SessionRepresentation session)
    {
        String expectedJobName = URLEncoder.encode(createTableQueryId, UTF_8);

        checkJobRegistration(client, expectedJobName);
    }

    @Override
    public void assertInsertIntoTable(
            String createTableQueryId,
            String createTableQuery,
            String insertQueryId,
            String insertQuery,
            String fullTableName,
            SessionRepresentation session)
    {
        {
            String expectedJobName = URLEncoder.encode(createTableQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
        {
            String expectedJobName = URLEncoder.encode(insertQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
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
        {
            String expectedJobName = URLEncoder.encode(createSchemaQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
        {
            String expectedJobName = URLEncoder.encode(createTableQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
        {
            String expectedJobName = URLEncoder.encode(deleteQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
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
        {
            String expectedJobName = URLEncoder.encode(createSchemaQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
        {
            String expectedJobName = URLEncoder.encode(createTableQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
        {
            String expectedJobName = URLEncoder.encode(mergeQueryId, UTF_8);
            checkJobRegistration(client, expectedJobName);
        }
    }

    private void checkJobRegistration(HttpClient client, String expectedJobName)
    {
        try {
            String encodedNamespace = URLEncoder.encode(OPEN_LINEAGE_NAMESPACE, UTF_8);
            HttpRequest requestJob = HttpRequest.newBuilder()
                    .uri(new URI(marquezURI + "/api/v1/namespaces/" + encodedNamespace + "/jobs/" + expectedJobName))
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
