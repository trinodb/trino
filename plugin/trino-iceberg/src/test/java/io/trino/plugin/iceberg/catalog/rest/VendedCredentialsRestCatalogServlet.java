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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RestCatalogServlet;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponseParser;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extends {@link RestCatalogServlet} to handle the {@code GET .../credentials} custom endpoint.
 */
public abstract class VendedCredentialsRestCatalogServlet
        extends RestCatalogServlet
{
    private final AtomicInteger vendedCredentialsRefreshCount = new AtomicInteger();

    public VendedCredentialsRestCatalogServlet(RESTCatalogAdapter restCatalogAdapter)
    {
        super(restCatalogAdapter);
    }

    public int getVendedCredentialsRefreshCount()
    {
        return vendedCredentialsRefreshCount.get();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        String path = request.getRequestURI().substring(1);
        if (path.equals("credentials")) {
            handleCredentialsRequest(response);
            return;
        }
        super.doGet(request, response);
    }

    private void handleCredentialsRequest(HttpServletResponse response)
            throws IOException
    {
        vendedCredentialsRefreshCount.incrementAndGet();
        LoadCredentialsResponse credentialsResponse = ImmutableLoadCredentialsResponse.builder()
                .addCredentials(ImmutableCredential.builder()
                        .prefix(getPrefix())
                        .putAllConfig(getCredentialsConfig())
                        .build())
                .build();

        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType());
        response.getWriter().write(LoadCredentialsResponseParser.toJson(credentialsResponse));
    }

    protected abstract String getPrefix();

    protected abstract Map<String, String> getCredentialsConfig();
}
