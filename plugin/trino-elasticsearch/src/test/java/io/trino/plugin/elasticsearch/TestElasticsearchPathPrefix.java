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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.nginx.NginxContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.PASSWORD;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.USER;
import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_7_IMAGE;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchPathPrefix
        extends AbstractTestQueryFramework
{
    private static final String PATH_PREFIX = "/elasticsearch";

    // Serves Elasticsearch only under the path prefix, like a reverse proxy shared with other services
    private static final String PROXY_CONFIGURATION =
            """
            server {
                listen 80;
                location %s/ {
                    proxy_pass https://elasticsearch-server:9200/;
                    proxy_ssl_verify off;
                }
                location / {
                    return 404;
                }
            }
            """.formatted(PATH_PREFIX);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Network network = closeAfterClass(Network.newNetwork());
        ElasticsearchServer elasticsearch = closeAfterClass(new ElasticsearchServer(network, ELASTICSEARCH_7_IMAGE));
        NginxContainer proxy = closeAfterClass(new NginxContainer(DockerImageName.parse("nginx:1.29-alpine"))
                .withNetwork(network)
                .withCopyToContainer(Transferable.of(PROXY_CONFIGURATION), "/etc/nginx/conf.d/default.conf"));
        proxy.start();

        Map<String, String> proxyProperties = ImmutableMap.of(
                "elasticsearch.host", proxy.getHost(),
                "elasticsearch.port", Integer.toString(proxy.getMappedPort(80)),
                "elasticsearch.tls.enabled", "false");

        QueryRunner queryRunner = ElasticsearchQueryRunner.builder(elasticsearch)
                .addConnectorProperties(proxyProperties)
                .addConnectorProperties(ImmutableMap.of("elasticsearch.path-prefix", PATH_PREFIX))
                .setInitialTables(ImmutableList.of(NATION))
                .build();

        queryRunner.createCatalog("elasticsearch_without_prefix", "elasticsearch", ImmutableMap.<String, String>builder()
                .putAll(proxyProperties)
                .put("elasticsearch.ignore-publish-address", "true")
                .put("elasticsearch.default-schema-name", "tpch")
                .put("elasticsearch.security", "PASSWORD")
                .put("elasticsearch.auth.user", USER)
                .put("elasticsearch.auth.password", PASSWORD)
                .buildOrThrow());

        return queryRunner;
    }

    @Test
    public void testQueryWithPathPrefix()
    {
        assertQuery("SELECT name FROM nation WHERE nationkey = 0", "VALUES 'ALGERIA'");
        assertQuery("SELECT count(*) FROM nation", "VALUES 25");
    }

    @Test
    public void testQueryWithoutPathPrefix()
    {
        assertThat(query("SELECT count(*) FROM elasticsearch_without_prefix.tpch.nation"))
                .failure().hasMessageContaining("Table 'elasticsearch_without_prefix.tpch.nation' does not exist");
    }
}
