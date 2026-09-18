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

public class IcebergS3RestCatalogBackendContainer
        extends BaseTestContainer
{
    public IcebergS3RestCatalogBackendContainer(
            Optional<Network> network,
            String warehouseLocation,
            String accessKey,
            String secretKey,
            String sessionToken,
            String endpoint,
            String region)
    {
        super("apache/iceberg-rest-fixture:1.10.1",
                "iceberg-rest",
                ImmutableSet.of(8181),
                ImmutableMap.of(),
                ImmutableMap.of(
                        "CATALOG_INCLUDE__CREDENTIALS", "true",
                        "CATALOG_WAREHOUSE", warehouseLocation,
                        "CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO",
                        "AWS_REGION", region,
                        "CATALOG_S3_ACCESS__KEY__ID", accessKey,
                        "CATALOG_S3_SECRET__ACCESS__KEY", secretKey,
                        "CATALOG_S3_SESSION__TOKEN", sessionToken,
                        "CATALOG_S3_ENDPOINT", endpoint,
                        "CATALOG_S3_PATH__STYLE__ACCESS", "true"),
                network,
                5);
    }

    public String getRestCatalogEndpoint()
    {
        return getMappedHostAndPortForExposedPort(8181).toString();
    }
}
