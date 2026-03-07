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

import java.util.Optional;

public class IcebergGcsRestCatalogBackendContainer
        extends BaseTestContainer
{
    public IcebergGcsRestCatalogBackendContainer(
            Optional<Network> network,
            String warehouseLocation,
            String gcpCredentialsFilePath,
            String gcpProjectId)
    {
        super(
                "apache/iceberg-rest-fixture:1.10.1",
                "iceberg-rest",
                ImmutableSet.of(8181),
                ImmutableMap.of("/gcs-credentials.json", gcpCredentialsFilePath),
                ImmutableMap.of(
                        "CATALOG_INCLUDE__CREDENTIALS", "true",
                        "CATALOG_WAREHOUSE", warehouseLocation,
                        "CATALOG_IO__IMPL", "org.apache.iceberg.gcp.gcs.GCSFileIO",
                        "CATALOG_GCS_PROJECT__ID", gcpProjectId,
                        "GOOGLE_APPLICATION_CREDENTIALS", "/gcs-credentials.json"),
                network,
                5);
    }

    public String getRestCatalogEndpoint()
    {
        return getMappedHostAndPortForExposedPort(8181).toString();
    }
}
