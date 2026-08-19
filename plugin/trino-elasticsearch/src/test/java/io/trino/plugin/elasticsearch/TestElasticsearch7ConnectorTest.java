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

import io.trino.Session;
import io.trino.sql.planner.plan.FilterNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_7_IMAGE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearch7ConnectorTest
        extends BaseElasticsearchConnectorTest
{
    public TestElasticsearch7ConnectorTest()
            throws IOException
    {
        super(new ElasticsearchServer(ELASTICSEARCH_7_IMAGE));
    }

    @Override
    @Test
    public void testRegexpLikeIsNotPushedDown()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        Session safe = Session.builder(getSession())
                .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "SAFE")
                .setCatalogSessionProperty(catalogName, "keyword_subfield_pushdown_with_ignore_above", "true")
                .build();
        Session unsafe = Session.builder(getSession())
                .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "UNSAFE")
                .setCatalogSessionProperty(catalogName, "keyword_subfield_pushdown_with_ignore_above", "true")
                .build();

        // Dynamic TPCH varchar mappings have an exact keyword sub-field. SAFE uses the translated regexp only as a
        // candidate pre-filter and keeps the Trino residual; UNSAFE makes Elasticsearch authoritative.
        assertThat(query(safe, "SELECT name FROM nation WHERE regexp_like(name, 'ALGERIA')"))
                .matches("VALUES VARCHAR 'ALGERIA'")
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query(unsafe, "SELECT name FROM nation WHERE regexp_like(name, 'ALGERIA')"))
                .matches("VALUES VARCHAR 'ALGERIA'")
                .isFullyPushedDown();
    }
}
