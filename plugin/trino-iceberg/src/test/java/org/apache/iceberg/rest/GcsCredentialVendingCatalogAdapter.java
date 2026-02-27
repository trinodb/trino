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
package org.apache.iceberg.rest;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.util.Map;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A {@link RESTCatalogAdapter} that injects GCS credential properties into
 * {@link LoadTableResponse} config, simulating a REST catalog server that
 * vends GCS credentials.
 */
public class GcsCredentialVendingCatalogAdapter
        extends RESTCatalogAdapter
{
    private final Map<String, String> gcsCredentialConfig;

    public GcsCredentialVendingCatalogAdapter(Catalog catalog, Map<String, String> gcsCredentialConfig)
    {
        super(catalog);
        this.gcsCredentialConfig = ImmutableMap.copyOf(requireNonNull(gcsCredentialConfig, "gcsCredentialConfig is null"));
    }

    @Override
    protected <T extends RESTResponse> T execute(
            HTTPRequest request,
            Class<T> responseType,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> configConsumer)
    {
        T response = super.execute(request, responseType, errorHandler, configConsumer);
        if (response instanceof LoadTableResponse loadTableResponse) {
            @SuppressWarnings("unchecked")
            T modified = (T) LoadTableResponse.builder()
                    .withTableMetadata(loadTableResponse.tableMetadata())
                    .addAllConfig(loadTableResponse.config())
                    .addAllConfig(gcsCredentialConfig)
                    .build();
            return modified;
        }
        return response;
    }
}
