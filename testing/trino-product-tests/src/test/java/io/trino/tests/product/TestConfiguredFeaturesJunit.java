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
package io.trino.tests.product;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.suite.SuiteTag;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ported from Tempto-based TestConfiguredFeatures.
 */
@ProductTest
@RequiresEnvironment(AllConnectorsSmokeEnvironment.class)
@TestGroup.ConfiguredFeatures
@SuiteTag.AllConnectorsSmoke
class TestConfiguredFeaturesJunit
{
    @Test
    void selectConfiguredConnectors(AllConnectorsSmokeEnvironment env)
    {
        assertThat(env.getConfiguredConnectors()).isNotEmpty();

        assertThat(env.executeTrino("SELECT DISTINCT connector_name FROM system.metadata.catalogs"))
                .containsOnly(env.getConfiguredConnectors().stream()
                        .map(connector -> row(connector))
                        .toList());
    }
}
