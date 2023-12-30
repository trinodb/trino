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
package io.trino.plugin.elasticsearch;

import com.google.common.net.HostAndPort;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;

import static java.lang.String.format;

public class TestElasticsearch8ConnectorTest
        extends BaseElasticsearchConnectorTest
{
    public TestElasticsearch8ConnectorTest()
    {
        super("elasticsearch:8.11.3", "elasticsearch8");
    }

    @Override
    protected RestHighLevelClient createElasticsearchClient(ElasticsearchServer esServer)
    {
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PASSWORD));

        HostAndPort address = esServer.getAddress();
        HttpHost httpsHost = new HttpHost(address.getHost(), address.getPort(), "https");
        RestClientBuilder builder = RestClient.builder(httpsHost)
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.setSSLContext(esServer.createSslContextFromCa());
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    return httpAsyncClientBuilder;
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        try {
            boolean ping = client.ping(RequestOptions.DEFAULT);
            Assertions.assertTrue(ping);
        }
        catch (IOException e) {
            Assertions.fail(e);
        }
        return client;
    }

    @Override
    protected String indexEndpoint(String index, String docId)
    {
        return format("/%s/_doc/%s", index, docId);
    }

    @Override
    protected String indexMapping(String properties)
    {
        return "{\"mappings\": " + properties + "}";
    }
}
