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

import java.util.Optional;

public class IcebergAzureRestCatalogBackendContainer
        extends BaseTestContainer
{
    public IcebergAzureRestCatalogBackendContainer(
            String warehouseLocation,
            String storageAccount,
            String storageAccountKey,
            String sasToken,
            long sasTokenExpiresAtMs)
    {
        super(
                "apache/iceberg-rest-fixture:1.10.1",
                "iceberg-rest",
                ImmutableSet.of(8181),
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("CATALOG_INCLUDE__CREDENTIALS", "true")
                        .put("CATALOG_WAREHOUSE", warehouseLocation)
                        .put("CATALOG_IO__IMPL", "org.apache.iceberg.azure.adlsv2.ADLSFileIO")
                        // Shared key auth for catalog operations
                        .put("CATALOG_ADLS_AUTH_SHARED__KEY_ACCOUNT_NAME", storageAccount)
                        .put("CATALOG_ADLS_AUTH_SHARED__KEY_ACCOUNT_KEY", storageAccountKey)
                        // SAS token for vending to clients
                        .put("CATALOG_ADLS_SAS__TOKEN_" + storageAccount, sasToken)
                        .put("CATALOG_ADLS_SAS__TOKEN__EXPIRES__AT__MS_" + storageAccount, Long.toString(sasTokenExpiresAtMs))
                        // Increase the size of the worker pool to achieve a higher parallelism for storage operations
                        .put("ICEBERG_WORKER_NUM_THREADS", "16")
                        .buildOrThrow(),
                Optional.empty(),
                5);
    }

    public String catalogUri()
    {
        return "http://" + getMappedHostAndPortForExposedPort(8181).toString();
    }
}
