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

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.util.Map;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Test-only {@link RESTCatalogAdapter} that injects a fixed set of {@code
 * fileIoProperties} entries into every {@link LoadTableResponse}. Simulates a
 * REST catalog that vends per-table storage configuration such as access
 * credentials or server-side encryption keys.
 */
public class VendingRESTCatalogAdapter
        extends RESTCatalogAdapter
{
    private final Map<String, String> vendedConfig;

    public VendingRESTCatalogAdapter(Catalog catalog, Map<String, String> vendedConfig)
    {
        super(catalog);
        this.vendedConfig = ImmutableMap.copyOf(requireNonNull(vendedConfig, "vendedConfig is null"));
    }

    @Override
    protected <T extends RESTResponse> T execute(
            HTTPRequest request,
            Class<T> responseType,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        T response = super.execute(request, responseType, errorHandler, responseHeaders);
        if (response instanceof LoadTableResponse loadTableResponse && !vendedConfig.isEmpty()) {
            return responseType.cast(
                    LoadTableResponse.builder()
                            .withTableMetadata(loadTableResponse.tableMetadata())
                            .addAllConfig(loadTableResponse.config())
                            .addAllConfig(vendedConfig)
                            .build());
        }
        return response;
    }
}
