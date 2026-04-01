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

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A test adapter that wraps {@link RESTCatalogAdapter} and injects fake S3 vended credentials
 * into every {@link LoadTableResponse}. Each call receives a unique access key so tests can
 * verify that a cache miss triggers a genuine re-fetch.
 * <p>
 * This lives in the {@code org.apache.iceberg.rest} package so it can access the
 * package-private {@link DelegatingRestSessionCatalog} constructor.
 */
public class CredentialVendingRESTCatalogAdapter
        extends RESTCatalogAdapter
{
    private final AtomicInteger loadTableCount = new AtomicInteger();

    public CredentialVendingRESTCatalogAdapter(Catalog delegate)
    {
        super(delegate);
    }

    public int getLoadTableCount()
    {
        return loadTableCount.get();
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
            int count = loadTableCount.incrementAndGet();
            TableMetadata metadata = loadTableResponse.tableMetadata();
            LoadTableResponse modified = LoadTableResponse.builder()
                    .withTableMetadata(metadata)
                    .addAllConfig(loadTableResponse.config())
                    .addConfig("s3.access-key-id", "FAKE_ACCESS_KEY_" + count)
                    .addConfig("s3.secret-access-key", "FAKE_SECRET_KEY_" + count)
                    .addConfig("s3.session-token", "FAKE_SESSION_TOKEN_" + count)
                    .addConfig("s3.session-token-expires-at-ms", Long.toString(Instant.now().plus(30, ChronoUnit.MINUTES).toEpochMilli()))
                    .build();
            return responseType.cast(modified);
        }
        return response;
    }

    /**
     * Creates a {@link DelegatingRestSessionCatalog} backed by this credential-vending adapter.
     */
    public static Bundle createBundle(Catalog delegate)
    {
        requireNonNull(delegate, "delegate is null");
        CredentialVendingRESTCatalogAdapter adapter = new CredentialVendingRESTCatalogAdapter(delegate);
        DelegatingRestSessionCatalog catalog = new DelegatingRestSessionCatalog(adapter, delegate);
        return new Bundle(catalog, adapter);
    }

    public record Bundle(DelegatingRestSessionCatalog catalog, CredentialVendingRESTCatalogAdapter adapter) {}
}
