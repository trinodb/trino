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

import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A {@link RESTClient} wrapper that intercepts {@link LoadTableResponse} results and merges
 * storage-credentials into the response's config map. This allows Trino's ioBuilder to receive
 * vended credentials through the normal config property flow, rather than requiring the
 * {@code SupportsStorageCredentials} interface that is bypassed when a custom ioBuilder is set.
 */
class StorageCredentialsMergingRestClient
        implements RESTClient
{
    private final RESTClient delegate;

    StorageCredentialsMergingRestClient(RESTClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
    {
        delegate.head(path, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return delegate.delete(path, responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return delegate.delete(path, queryParams, responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        T response = delegate.get(path, queryParams, responseType, headers, errorHandler);
        return mergeStorageCredentials(response, responseType);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        T response = delegate.post(path, body, responseType, headers, errorHandler);
        return mergeStorageCredentials(response, responseType);
    }

    @Override
    public <T extends RESTResponse> T postForm(
            String path,
            Map<String, String> formData,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return delegate.postForm(path, formData, responseType, headers, errorHandler);
    }

    @Override
    public RESTClient withAuthSession(AuthSession session)
    {
        return new StorageCredentialsMergingRestClient(delegate.withAuthSession(session));
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @SuppressWarnings("unchecked")
    private <T extends RESTResponse> T mergeStorageCredentials(T response, Class<T> responseType)
    {
        if (responseType != LoadTableResponse.class) {
            return response;
        }

        LoadTableResponse tableResponse = (LoadTableResponse) response;
        List<Credential> credentials = tableResponse.credentials();
        if (credentials.isEmpty()) {
            return response;
        }

        String tableLocation = tableResponse.tableMetadata().location();
        Credential bestMatch = findBestMatchingCredential(credentials, tableLocation);
        if (bestMatch == null) {
            return response;
        }

        Map<String, String> mergedConfig = new HashMap<>(tableResponse.config());
        mergedConfig.putAll(bestMatch.config());

        return (T) LoadTableResponse.builder()
                .withTableMetadata(tableResponse.tableMetadata())
                .addAllConfig(mergedConfig)
                .addAllCredentials(credentials)
                .build();
    }

    /**
     * Finds the credential with the longest matching prefix for the given table location.
     * This matches the algorithm used by Iceberg's S3FileIO and GCSFileIO for selecting
     * per-prefix storage clients.
     */
    private static Credential findBestMatchingCredential(List<Credential> credentials, String tableLocation)
    {
        Credential best = null;
        int bestLength = -1;
        for (Credential credential : credentials) {
            String prefix = credential.prefix();
            if (tableLocation.startsWith(prefix) && prefix.length() > bestLength) {
                best = credential;
                bestLength = prefix.length();
            }
        }
        return best;
    }
}
