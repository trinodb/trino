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

import static io.trino.testing.containers.Minio.MINIO_REGION;

public class IcebergRestCatalogBackendContainer
        extends BaseTestContainer
{
    public IcebergRestCatalogBackendContainer(
            Optional<Network> network,
            String warehouseLocation,
            String minioAccessKey,
            String minioSecretKey,
            String minioSessionToken)
    {
        super(
                "tabulario/iceberg-rest:1.6.0",
                "iceberg-rest",
                ImmutableSet.of(8181),
                ImmutableMap.of(),
                toCatalogEnvVars(ImmutableMap.of(
                        "include-credentials", "true",
                        "warehouse", warehouseLocation,
                        "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                        "s3.access-key-id", minioAccessKey,
                        "s3.secret-access-key", minioSecretKey,
                        "s3.session-token", minioSessionToken,
                        "s3.endpoint", "http://minio:4566",
                        "s3.path-style-access", "true",
                        "client.region", MINIO_REGION)),
                network,
                5);
    }

    public String getRestCatalogEndpoint()
    {
        return getMappedHostAndPortForExposedPort(8181).toString();
    }

    private static Map<String, String> toCatalogEnvVars(Map<String, String> properties)
    {
        ImmutableMap.Builder<String, String> envVars = ImmutableMap.builder();
        properties.forEach((key, value) -> {
            String envVarName = "CATALOG_" +
                    key.toUpperCase(Locale.ROOT)
                            .replaceAll("-", "__")
                            .replaceAll("\\.", "_");

            envVars.put(envVarName, value);
        });
        return envVars.buildOrThrow();
    }
}
