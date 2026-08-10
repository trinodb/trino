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

import io.trino.testing.containers.environment.ProductTestEnvironment;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

public final class ConfiguredFeatures
{
    private static final Set<String> DEFAULT_CONNECTORS = Set.of("jmx", "memory", "system", "tpcds", "tpch");

    private ConfiguredFeatures() {}

    public static void assertDefaultConnectors(ProductTestEnvironment environment, String... additionalConnectors)
    {
        Set<String> expectedConnectors = new LinkedHashSet<>(DEFAULT_CONNECTORS);
        expectedConnectors.addAll(Arrays.asList(additionalConnectors));
        assertConnectors(environment, expectedConnectors);
    }

    public static void assertConnectors(ProductTestEnvironment environment, Collection<String> expectedConnectors)
    {
        assertThat(environment.executeTrino("SELECT DISTINCT connector_name FROM system.metadata.catalogs"))
                .containsOnly(expectedConnectors.stream()
                        .map(connector -> row(connector))
                        .toList());
    }
}
