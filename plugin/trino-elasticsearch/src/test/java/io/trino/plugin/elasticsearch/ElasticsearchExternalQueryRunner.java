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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static java.lang.Integer.parseInt;

public class ElasticsearchExternalQueryRunner
{
    private static final String HOSTNAME = System.getProperty("elasticsearch.host", "localhost");
    private static final int PORT = parseInt(System.getProperty("elasticsearch.port", "9200"));

    private ElasticsearchExternalQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        // Please set hostname and port via VM options. e.g. "-Delasticsearch.host=localhost -Delasticsearch.port=9200"
        // To start Elasticsearch:
        // docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.6.2
        DistributedQueryRunner queryRunner = createElasticsearchQueryRunner(
                HostAndPort.fromParts(HOSTNAME, PORT),
                TpchTable.getTables(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                3);

        Logger log = Logger.get(ElasticsearchExternalQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
