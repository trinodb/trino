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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static io.trino.plugin.prometheus.PrometheusServer.LATEST_VERSION;
import static io.trino.plugin.prometheus.PrometheusServer.PASSWORD;
import static io.trino.plugin.prometheus.PrometheusServer.USER;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrometheusBasicAuth
        extends AbstractTestQueryFramework
{
    private PrometheusServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new PrometheusServer(LATEST_VERSION, true));
        return createPrometheusQueryRunner(server, ImmutableMap.of(), ImmutableMap.of("prometheus.auth.user", USER, "prometheus.auth.password", PASSWORD));
    }

    @Test
    public void testSelect()
    {
        assertEventually(
                new Duration(1, MINUTES),
                new Duration(1, SECONDS),
                () -> assertQuery("SHOW TABLES IN prometheus.default LIKE 'up'", "VALUES 'up'"));

        assertQuery("SELECT labels['job'] FROM prometheus.default.up LIMIT 1", "VALUES 'prometheus'");
    }

    @Test
    public void testInvalidCredential()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.getUri());
        config.setUser("invalid-user");
        config.setPassword("invalid-password");
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> client.getTableNames("default"))
                .hasMessage("Bad response 401 Unauthorized");
    }

    @Test
    public void testMissingCredential()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.getUri());
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> client.getTableNames("default"))
                .hasMessage("Bad response 401 Unauthorized");
    }
}
