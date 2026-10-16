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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSigningProperties.AUTHENTICATION_PROPERTY;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSigningProperties.encodeAuthenticationProperties;
import static java.util.Objects.requireNonNull;

final class IcebergRestCatalogS3RemoteSigningClient
        implements RESTClient
{
    private final RESTClient delegate;
    private final Map<String, String> catalogProperties;
    private final AuthSession authSession;

    IcebergRestCatalogS3RemoteSigningClient(Map<String, String> catalogProperties)
    {
        this(HTTPClient.builder(catalogProperties)
                        .uri(catalogProperties.get(CatalogProperties.URI))
                        .withHeaders(RESTUtil.configHeaders(catalogProperties))
                        .withAuthSession(AuthSession.EMPTY)
                        .build(),
                catalogProperties,
                AuthSession.EMPTY);
    }

    private IcebergRestCatalogS3RemoteSigningClient(RESTClient delegate, Map<String, String> catalogProperties, AuthSession authSession)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.catalogProperties = ImmutableMap.copyOf(catalogProperties);
        this.authSession = requireNonNull(authSession, "authSession is null");
    }

    @Override
    public RESTClient withAuthSession(AuthSession session)
    {
        return new IcebergRestCatalogS3RemoteSigningClient(delegate.withAuthSession(session), catalogProperties, session);
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
        return withAuthenticationProperties(delegate.delete(path, responseType, headers, errorHandler), responseType);
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return withAuthenticationProperties(delegate.delete(path, queryParams, responseType, headers, errorHandler), responseType);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return withAuthenticationProperties(delegate.get(path, queryParams, responseType, headers, errorHandler), responseType);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        return withAuthenticationProperties(delegate.get(path, queryParams, responseType, headers, errorHandler, responseHeaders), responseType);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            ParserContext parserContext)
    {
        return withAuthenticationProperties(delegate.get(path, queryParams, responseType, headers, errorHandler, parserContext), responseType);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return withAuthenticationProperties(delegate.post(path, body, responseType, headers, errorHandler), responseType);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        return withAuthenticationProperties(delegate.post(path, body, responseType, headers, errorHandler, responseHeaders), responseType);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders,
            ParserContext parserContext)
    {
        return withAuthenticationProperties(delegate.post(path, body, responseType, headers, errorHandler, responseHeaders, parserContext), responseType);
    }

    @Override
    public <T extends RESTResponse> T postForm(
            String path,
            Map<String, String> formData,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return withAuthenticationProperties(delegate.postForm(path, formData, responseType, headers, errorHandler), responseType);
    }

    private <T extends RESTResponse> T withAuthenticationProperties(T response, Class<T> responseType)
    {
        if (!(response instanceof LoadTableResponse table)) {
            return response;
        }
        return responseType.cast(LoadTableResponse.builder()
                .withTableMetadata(table.tableMetadata())
                .addAllConfig(table.config())
                .addConfig(AUTHENTICATION_PROPERTY, encodeAuthenticationProperties(catalogProperties, table.config(), authSession))
                .addAllCredentials(table.credentials())
                .withRemoteSigningConfig(table.remoteSigningConfig())
                .withLabels(table.labels())
                .build());
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
