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
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Opts every table into REST server-side scan planning by adding
 * {@code scan-planning-mode=server} to load table responses, and counts plan requests.
 */
public class ScanPlanningRestCatalogAdapter
        extends RESTCatalogAdapter
{
    private final AtomicInteger planRequests = new AtomicInteger();

    public ScanPlanningRestCatalogAdapter(Catalog delegate)
    {
        super(delegate);
    }

    public int planRequests()
    {
        return planRequests.get();
    }

    @Override
    protected <T extends RESTResponse> T execute(
            HTTPRequest request,
            Class<T> responseType,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders,
            ParserContext parserContext)
    {
        if ("POST".equals(request.method().name()) && request.path().endsWith("/plan")) {
            planRequests.incrementAndGet();
        }
        T response = super.execute(request, responseType, errorHandler, responseHeaders, parserContext);
        if (response instanceof LoadTableResponse loadTableResponse) {
            LoadTableResponse modified = LoadTableResponse.builder()
                    .withTableMetadata(loadTableResponse.tableMetadata())
                    .addAllConfig(loadTableResponse.config())
                    .addAllConfig(ImmutableMap.of("scan-planning-mode", "server"))
                    .build();
            return responseType.cast(modified);
        }
        return response;
    }
}
