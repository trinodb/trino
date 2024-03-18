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
package io.trino.plugin.varada.configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxiedConnectorConfigurationTest
{
    @Test
    public void testPassThroughDispatcherSet()
    {
        ProxiedConnectorConfiguration proxiedConnectorConfiguration = new ProxiedConnectorConfiguration();

        proxiedConnectorConfiguration.setPassThroughDispatcherSet(
                String.format(" %s , %s ", ProxiedConnectorConfiguration.HUDI_CONNECTOR_NAME, ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME));
        assertThat(proxiedConnectorConfiguration.getPassThroughDispatcherSet())
                .containsExactlyInAnyOrder(ProxiedConnectorConfiguration.HUDI_CONNECTOR_NAME, ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME);

        proxiedConnectorConfiguration.setPassThroughDispatcherSet(
                String.format(" %s,%s, %s ",
                        ProxiedConnectorConfiguration.HUDI_CONNECTOR_NAME,
                        ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME,
                        ProxiedConnectorConfiguration.ICEBERG_CONNECTOR_NAME));
        assertThat(proxiedConnectorConfiguration.getPassThroughDispatcherSet())
                .containsExactlyInAnyOrder(
                        ProxiedConnectorConfiguration.HUDI_CONNECTOR_NAME,
                        ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME,
                        ProxiedConnectorConfiguration.ICEBERG_CONNECTOR_NAME);

        assertThatThrownBy(() -> proxiedConnectorConfiguration.setPassThroughDispatcherSet(" hive11,hudi, iceberg "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("enable.passthrough configuration only supports");
    }
}
