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
package io.trino.testing.containers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testcontainers.containers.Network;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

// TODO: Reduce duplication
public class IcebergAzureRestCatalogBackendContainer
        extends BaseTestContainer
{
    public IcebergAzureRestCatalogBackendContainer(
            Optional<Network> network,
            String warehouseLocation,
            String accountName,
            String sasToken)
    {
        super(
                "tabulario/iceberg-rest:1.6.0",
                "iceberg-rest",
                ImmutableSet.of(8181),
                ImmutableMap.of(),
                toCatalogEnvVars(ImmutableMap.of(
                        "include-credentials", "true",
                        "warehouse", warehouseLocation,
                        "io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
                        "adls.sas-token." + accountName + ".dfs.core.windows.net", sasToken)),
                network,
                5);
    }

    public String getRestCatalogEndpoint()
    {
        return getMappedHostAndPortForExposedPort(8181).toString();
    }

    private static Map<String, String> toCatalogEnvVars(Map<String, String> catalogProperties)
    {
        ImmutableMap.Builder<String, String> envVarProperties = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            String envVarKey = "CATALOG_" +
                    entry.getKey()
                            .toUpperCase(Locale.ROOT)
                            .replaceAll("-", "__")
                            .replaceAll("\\.", "_");

            envVarProperties.put(envVarKey, entry.getValue());
        }
        return envVarProperties.build();
    }
}
