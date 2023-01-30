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

import com.amazonaws.util.Base64;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.net.HostAndPort;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPasswordAuthentication
{
    private static final String USER = "elastic_user";
    private static final String PASSWORD = "123456";

    private ElasticsearchServer elasticsearch;
    private RestHighLevelClient client;
    private QueryAssertions assertions;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        // We use 7.8.0 because security became a non-commercial feature in recent versions
        elasticsearch = new ElasticsearchServer("elasticsearch:7.8.0", ImmutableMap.<String, String>builder()
                .put("elasticsearch.yml", loadResource("elasticsearch.yml"))
                .put("users", loadResource("users"))
                .put("users_roles", loadResource("users_roles"))
                .put("roles.yml", loadResource("roles.yml"))
                .buildOrThrow());

        HostAndPort address = elasticsearch.getAddress();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(address.getHost(), address.getPort())));

        DistributedQueryRunner runner = createElasticsearchQueryRunner(
                elasticsearch.getAddress(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("elasticsearch.security", "PASSWORD")
                        .put("elasticsearch.auth.user", USER)
                        .put("elasticsearch.auth.password", PASSWORD)
                        .buildOrThrow(),
                3);

        assertions = new QueryAssertions(runner);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        closeAll(
                () -> assertions.close(),
                () -> elasticsearch.stop(),
                () -> client.close());

        assertions = null;
        elasticsearch = null;
        client = null;
    }

    @Test
    public void test()
            throws IOException
    {
        String json = new ObjectMapper().writeValueAsString(ImmutableMap.of("value", 42L));

        client.getLowLevelClient()
                .performRequest(
                        "POST",
                        "/test/_doc?refresh",
                        ImmutableMap.of(),
                        new NStringEntity(json, ContentType.APPLICATION_JSON),
                        new BasicHeader("Authorization", format("Basic %s", Base64.encodeAsString(format("%s:%s", USER, PASSWORD).getBytes(StandardCharsets.UTF_8)))));

        assertThat(assertions.query("SELECT * FROM test"))
                .matches("VALUES BIGINT '42'");
    }

    private static String loadResource(String file)
            throws IOException
    {
        return Resources.toString(getResource(file), UTF_8);
    }
}
