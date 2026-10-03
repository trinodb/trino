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
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.util.Map;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A REST catalog adapter that injects S3 vended credential properties
 * into the LoadTableResponse config, simulating a REST catalog server
 * that vends S3 credentials with region, endpoint, and cross-region access.
 */
public class S3CredentialVendingCatalogAdapter
        extends RESTCatalogAdapter
{
    private final Map<String, String> vendedS3Properties;

    public S3CredentialVendingCatalogAdapter(Catalog backendCatalog, Map<String, String> vendedS3Properties)
    {
        super(backendCatalog);
        this.vendedS3Properties = requireNonNull(vendedS3Properties, "vendedS3Properties is null");
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
            loadTableResponse.config().putAll(vendedS3Properties);
        }
        return response;
    }
}
