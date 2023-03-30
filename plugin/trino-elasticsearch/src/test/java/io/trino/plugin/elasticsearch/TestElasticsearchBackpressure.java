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
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static io.trino.tpch.TpchTable.ORDERS;

public class TestElasticsearchBackpressure
        extends AbstractTestQueryFramework
{
    private static final String image = "elasticsearch:7.0.0";

    private Network network;
    private ElasticsearchServer elasticsearch;
    private ElasticsearchNginxProxy elasticsearchNginxProxy;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        network = Network.newNetwork();
        elasticsearch = new ElasticsearchServer(network, image, ImmutableMap.of());
        elasticsearchNginxProxy = new ElasticsearchNginxProxy(network, 1);

        return createElasticsearchQueryRunner(
                elasticsearchNginxProxy.getAddress(),
                ImmutableList.of(ORDERS),
                ImmutableMap.of(),
                ImmutableMap.of(),
                // This test can only run on a single node, otherwise each node exports its own stats beans and they override each other
                // You can only bind one such bean per JVM, so this causes problems with statistics being 0 despite backpressure handling
                1,
                // Use a unique catalog name to make sure JMX stats beans are unique and not affected by other tests
                "elasticsearch-backpressure");
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        elasticsearchNginxProxy.stop();
        elasticsearchNginxProxy = null;
        elasticsearch.stop();
        elasticsearch = null;
        network.close();
        network = null;
    }

    @Test
    public void testQueryWithBackpressure()
    {
        // Check that JMX stats show no sign of backpressure
        assertQueryReturnsEmptyResult("SELECT 1 FROM jmx.current.\"trino.plugin.elasticsearch.client:*\" WHERE \"backpressurestats.alltime.count\" > 0");
        assertQueryReturnsEmptyResult("SELECT 1 FROM jmx.current.\"trino.plugin.elasticsearch.client:*\" WHERE \"backpressurestats.alltime.max\" > 0");

        assertQuerySucceeds("SELECT * FROM orders");

        // Check that JMX stats show requests have been retried due to backpressure
        assertQuery("SELECT DISTINCT 1 FROM jmx.current.\"trino.plugin.elasticsearch.client:*\" WHERE \"backpressurestats.alltime.count\" > 0", "VALUES 1");
        assertQuery("SELECT DISTINCT 1 FROM jmx.current.\"trino.plugin.elasticsearch.client:*\" WHERE \"backpressurestats.alltime.max\" > 0", "VALUES 1");
    }
}
