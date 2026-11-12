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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.net.HostAndPort;
import io.trino.plugin.iceberg.TrinoMetricsReporter;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;

import static org.apache.iceberg.CatalogUtil.loadMetricsReporter;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRestCatalogPropertiesProvider
{
    @Test
    void testMetricsReporter()
    {
        IcebergRestCatalogPropertiesProvider catalogPropertiesProvider = new IcebergRestCatalogPropertiesProvider(
                new IcebergRestCatalogConfig().setBaseUri("http://localhost"),
                new NoneSecurityProperties(),
                new NodeVersion("test"));

        assertThat(loadMetricsReporter(catalogPropertiesProvider.catalogProperties()))
                .isInstanceOf(TrinoMetricsReporter.class);
    }

    @Test
    void testProxyProperties()
    {
        assertThat(new IcebergRestCatalogPropertiesProvider(
                new IcebergRestCatalogConfig().setBaseUri("http://localhost"),
                new NoneSecurityProperties(),
                new NodeVersion("test")).catalogProperties())
                .doesNotContainKeys("rest.client.proxy.hostname", "rest.client.proxy.port", "rest.client.proxy.username", "rest.client.proxy.password");

        assertThat(new IcebergRestCatalogPropertiesProvider(
                new IcebergRestCatalogConfig()
                        .setBaseUri("http://localhost")
                        .setHttpProxy(HostAndPort.fromParts("proxy.example.com", 3128))
                        .setProxyUsername("proxy-user")
                        .setProxyPassword("proxy-password"),
                new NoneSecurityProperties(),
                new NodeVersion("test")).catalogProperties())
                .containsEntry("rest.client.proxy.hostname", "proxy.example.com")
                .containsEntry("rest.client.proxy.port", "3128")
                .containsEntry("rest.client.proxy.username", "proxy-user")
                .containsEntry("rest.client.proxy.password", "proxy-password");
    }
}
