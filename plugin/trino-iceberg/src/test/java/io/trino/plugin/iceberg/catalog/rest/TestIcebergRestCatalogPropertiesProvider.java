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

import io.trino.plugin.iceberg.TrinoMetricsReporter;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.apache.iceberg.CatalogUtil.loadMetricsReporter;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRestCatalogPropertiesProvider
{
    @Test
    void testMetricsReporter()
    {
        IcebergRestCatalogPropertiesProvider catalogPropertiesProvider = new IcebergRestCatalogPropertiesProvider(
                new IcebergRestCatalogConfig().setBaseUri("http://localhost"),
                Optional.empty(),
                new NoneSecurityProperties(),
                new NodeVersion("test"));

        assertThat(loadMetricsReporter(catalogPropertiesProvider.catalogProperties()))
                .isInstanceOf(TrinoMetricsReporter.class);
    }

    @Test
    void testAccessDelegationHeader()
    {
        assertThat(catalogProperties(false, Optional.empty())).doesNotContainKey("header.X-Iceberg-Access-Delegation");
        assertThat(catalogProperties(true, Optional.empty())).containsEntry("header.X-Iceberg-Access-Delegation", "vended-credentials");

        Optional<IcebergRestCatalogS3Config> disabled = Optional.of(new IcebergRestCatalogS3Config());
        assertThat(catalogProperties(false, disabled)).doesNotContainKey("header.X-Iceberg-Access-Delegation");
        assertThat(catalogProperties(true, disabled)).containsEntry("header.X-Iceberg-Access-Delegation", "vended-credentials");

        Optional<IcebergRestCatalogS3Config> enabled = Optional.of(new IcebergRestCatalogS3Config().setRemoteSigningEnabled(true));
        assertThat(catalogProperties(false, enabled)).containsEntry("header.X-Iceberg-Access-Delegation", "remote-signing");
    }

    private static Map<String, String> catalogProperties(boolean vendedCredentialsEnabled, Optional<IcebergRestCatalogS3Config> s3Config)
    {
        return new IcebergRestCatalogPropertiesProvider(
                new IcebergRestCatalogConfig().setBaseUri("http://localhost").setVendedCredentialsEnabled(vendedCredentialsEnabled),
                s3Config,
                new NoneSecurityProperties(),
                new NodeVersion("test"))
                .catalogProperties();
    }
}
