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

import static java.lang.String.format;

public class TestElasticsearchOpenSearchConnectorTest
        extends BaseElasticsearchConnectorTest
{
    public TestElasticsearchOpenSearchConnectorTest()
    {
        // 1.0.0 and 1.0.1 causes NotSslRecordException during the initialization
        super("opensearchproject/opensearch:1.1.0", "opensearch");
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
