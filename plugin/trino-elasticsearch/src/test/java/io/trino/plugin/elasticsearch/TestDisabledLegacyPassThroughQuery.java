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
import com.google.common.io.BaseEncoding;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_7_IMAGE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestDisabledLegacyPassThroughQuery
        extends AbstractTestQueryFramework
{
    private ElasticsearchServer elasticsearch;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer(ELASTICSEARCH_7_IMAGE, ImmutableMap.of());

        return createElasticsearchQueryRunner(
                elasticsearch.getAddress(),
                TpchTable.getTables(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                1,
                "elasticsearch-legacy-pass-through-query");
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        elasticsearch.stop();
        elasticsearch = null;
    }

    @Test
    public void testDisabledPassthroughQuery()
    {
        @Language("JSON")
        String query = "{\n" +
                "    \"size\": 0,\n" +
                "    \"aggs\" : {\n" +
                "        \"max_orderkey\" : { \"max\" : { \"field\" : \"orderkey\" } },\n" +
                "        \"sum_orderkey\" : { \"sum\" : { \"field\" : \"orderkey\" } }\n" +
                "    }\n" +
                "}";

        assertQueryFails(
                format("WITH data(r) AS (" +
                        "   SELECT CAST(json_parse(result) AS ROW(aggregations ROW(max_orderkey ROW(value BIGINT), sum_orderkey ROW(value BIGINT)))) " +
                        "   FROM \"orders$query:%s\") " +
                        "SELECT r.aggregations.max_orderkey.value, r.aggregations.sum_orderkey.value " +
                        "FROM data", BaseEncoding.base32().encode(query.getBytes(UTF_8))),
                "Pass-through query not supported\\. Please turn it on explicitly using elasticsearch\\.legacy-pass-through-query\\.enabled feature toggle");
    }
}
