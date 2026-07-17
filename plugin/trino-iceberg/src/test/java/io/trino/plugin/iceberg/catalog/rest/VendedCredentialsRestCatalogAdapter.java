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

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.util.Map;
import java.util.function.Consumer;

public abstract class VendedCredentialsRestCatalogAdapter
        extends RESTCatalogAdapter
{
    public VendedCredentialsRestCatalogAdapter(Catalog delegate)
    {
        super(delegate);
    }

    @Override
    protected <T extends RESTResponse> T execute(
            HTTPRequest request,
            Class<T> responseType,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        T response = super.execute(request, responseType, errorHandler, responseHeaders);
        if (response instanceof LoadTableResponse loadTableResponse) {
            TableMetadata metadata = loadTableResponse.tableMetadata();

            String host = request.headers().entries("host").stream()
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Host header not found in request"))
                    .value();
            String restServerUri = "http://" + host;

            LoadTableResponse modified = LoadTableResponse.builder()
                    .withTableMetadata(metadata)
                    .addAllConfig(loadTableResponse.config())
                    .addAllConfig(getVendedCredentialsConfig(restServerUri))
                    .build();
            return responseType.cast(modified);
        }
        return response;
    }

    protected abstract Map<String, String> getVendedCredentialsConfig(String restServerUri);
}
