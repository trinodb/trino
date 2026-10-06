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

package org.apache.iceberg.rest;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Path;

import static io.airlift.http.client.HeaderNames.ACCEPT_ENCODING;
import static io.airlift.http.client.HeaderNames.ETAG;
import static io.airlift.http.client.HeaderNames.IF_NONE_MATCH;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static jakarta.servlet.http.HttpServletResponse.SC_NOT_MODIFIED;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDelegatingRestSessionCatalog
{
    @Test
    void testETagConditionalRequests(@TempDir Path tempDir)
            throws Exception
    {
        try (HttpClient client = new JettyHttpClient();
                JdbcCatalog backendCatalog = (JdbcCatalog) backendCatalog(tempDir);
                DelegatingRestSessionCatalog restCatalog = DelegatingRestSessionCatalog.builder()
                        .delegate(backendCatalog)
                        .build()) {
            TableIdentifier table = createTable(backendCatalog);
            TestingHttpServer server = restCatalog.testServer();
            try {
                server.start();

                URI tableUri = tableUri(server, table);
                StringResponseHandler.StringResponse firstResponse = client.execute(prepareGet(tableUri).build(), createStringResponseHandler());
                assertThat(firstResponse.getStatusCode()).isEqualTo(SC_OK);

                StringResponseHandler.StringResponse secondResponse = client.execute(prepareGet(tableUri)
                        .setHeader(IF_NONE_MATCH, firstResponse.getHeader(ETAG).orElseThrow())
                        .build(), createStringResponseHandler());
                assertThat(secondResponse.getStatusCode()).isEqualTo(SC_NOT_MODIFIED);
            }
            finally {
                server.stop();
            }
        }
    }

    private static TableIdentifier createTable(JdbcCatalog backendCatalog)
    {
        Namespace namespace = Namespace.of("test_quoted_etag_" + randomNameSuffix());
        TableIdentifier table = TableIdentifier.of(namespace, "test_table");
        backendCatalog.createNamespace(namespace);
        backendCatalog.createTable(
                table,
                new Schema(required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(),
                ImmutableMap.of("padding", "x".repeat(4096)));
        return table;
    }

    private static Request.Builder prepareGet(URI tableUri)
    {
        return Request.Builder.prepareGet()
                .setUri(tableUri)
                .setHeader(ACCEPT_ENCODING, "gzip");
    }

    private static URI tableUri(TestingHttpServer server, TableIdentifier table)
    {
        return server.getBaseUrl().resolve("/" + ResourcePaths.forCatalogProperties(ImmutableMap.of()).table(table));
    }
}
