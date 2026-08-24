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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public abstract class BaseElasticsearchParallelConnectorTest
        extends BaseElasticsearchPredicateCompositionTest
{
    protected BaseElasticsearchParallelConnectorTest(ElasticsearchServer server)
    {
        super(server);
    }

    @Test
    @Override
    @Disabled("Covered by the isolated Elasticsearch metadata suite")
    public void testSelectInformationSchemaTables() {}

    @Test
    @Override
    @Disabled("Covered by the isolated Elasticsearch metadata suite")
    public void testSelectInformationSchemaColumns() {}

    @Test
    @Override
    @Disabled("Covered by the isolated Elasticsearch metadata suite")
    public void testSelectInformationSchemaForMultiIndexAlias() {}

    @Test
    @Override
    @Disabled("Covered by the isolated Elasticsearch metadata suite")
    public void testAsRawJsonAndIsArraySameFieldException() {}

    @Test
    @Override
    @Disabled("Covered by the isolated Elasticsearch metadata suite")
    public void testEmptyIndexWithMappings() {}

    @Test
    @Override
    @Disabled("Covered by the isolated Elasticsearch metadata suite")
    public void testEmptyIndexNoMappings() {}

    @Test
    @Override
    @Disabled("Covered by the isolated Elasticsearch metadata suite")
    public void testEmptyAliasNoMappings() {}

    @Test
    @Override
    @Disabled("Covered by the isolated Elasticsearch metadata suite")
    public void testMissingIndex() {}
}
