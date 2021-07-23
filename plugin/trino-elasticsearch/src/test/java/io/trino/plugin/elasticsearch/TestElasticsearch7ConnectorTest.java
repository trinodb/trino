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
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.elasticsearch.client.RestHighLevelClient;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static java.lang.String.format;

public class TestElasticsearch7ConnectorTest
        extends BaseElasticsearchConnectorTest
{
    private RestHighLevelClient client;

    @Override
    protected RestHighLevelClient getClient()
    {
        return client;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ElasticsearchServer elasticsearch = closeAfterClass(new ElasticsearchServer("elasticsearch:7.0.0", ImmutableMap.of()));
        client = closeAfterClass(elasticsearch.createClient());
        return createElasticsearchQueryRunner(elasticsearch.getAddress(), TpchTable.getTables(), ImmutableMap.of(), ImmutableMap.of(), 3);
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
