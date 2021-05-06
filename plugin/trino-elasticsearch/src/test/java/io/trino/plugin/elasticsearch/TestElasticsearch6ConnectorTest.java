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
import io.trino.sql.planner.plan.AggregationNode;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.IOException;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearch6ConnectorTest
        extends BaseElasticsearchConnectorTest
{
    public TestElasticsearch6ConnectorTest()
    {
        super("docker.elastic.co/elasticsearch/elasticsearch:6.0.0");
    }

    @Test
    public void testIndexWithMappingsButNoProperties()
            throws IOException
    {
        String indexName = "test_empty_index_with_mappings_no_properties";

        @Language("JSON")
        String mappings = "{\"mappings\": " +
                "  {\"foo\": { \"dynamic\" : \"strict\" } }" +
                "}";
        client.getLowLevelClient()
                .performRequest("PUT", "/" + indexName, ImmutableMap.of(), new NStringEntity(mappings, ContentType.APPLICATION_JSON));

        assertTableDoesNotExist(indexName);
    }

    @Test
    public void testNoAggregationPushdown()
    {
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(AggregationNode.class);
    }

    @Override
    protected String indexEndpoint(String index, String docId)
    {
        return format("/%s/doc/%s", index, docId);
    }

    @Override
    protected String indexMapping(String properties)
    {
        return "{\"mappings\": " +
                "  {\"doc\": " + properties + "}" +
                "}";
    }
}
