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
package io.prestosql.elasticsearch.client.protocols;

import static java.lang.String.format;

public class Elasticsearch6ProtocolConfig
        implements ElasticsearchProtocolConfig
{
    @Override
    public String indexEndpoint(String index, String docId)
    {
        return format("/%s/doc/%s", index, docId);
    }

    @Override
    public String indexBulk(String index)
    {
        return format("/%s/doc/_bulk?refresh", index);
    }

    @Override
    public String indexMapping(String properties)
    {
        return "{\"mappings\": " +
                "  {\"doc\": " + properties + "}" +
                "}";
    }

    @Override
    public String indexTemplate(String index, String properties)
    {
        return "{"
                + "\"index_patterns\": [\"" + index + "\"],"
                + "\"settings\": { "
                + "  \"number_of_shards\": 1,"
                + "  \"auto_expand_replicas\": \"0-1\""
                + "},"
                + "\"mappings\": " +
                "  {\"doc\": " + properties + "}" +
                "}";
    }
}
