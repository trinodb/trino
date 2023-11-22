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

import org.elasticsearch.client.Request;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static java.lang.String.format;

public class TestElasticsearch6ConnectorTest
        extends BaseElasticsearchConnectorTest
{
    public TestElasticsearch6ConnectorTest()
    {
        super("docker.elastic.co/elasticsearch/elasticsearch-oss:6.6.0", "elasticsearch6");
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
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(mappings);
        client.getLowLevelClient().performRequest(request);

        assertTableDoesNotExist(indexName);
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
