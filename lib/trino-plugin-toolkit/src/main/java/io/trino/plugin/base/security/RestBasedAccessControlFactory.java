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

package io.trino.plugin.base.security;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.trino.plugin.base.CatalogName;

import java.net.URI;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RestBasedAccessControlFactory
{
    private final CatalogName catalogName;
    private final HttpClient httpClient;
    private final URI configUri;

    public RestBasedAccessControlFactory(final CatalogName catalogName, final HttpClient httpClient, final URI configUri)
    {
        requireNonNull(catalogName);
        requireNonNull(httpClient);
        this.catalogName = catalogName;
        this.httpClient = httpClient;
        this.configUri = configUri;
    }

    private AccessControlRules restBasedRulesProvider()
    {
        Request request = prepareGet().setUri(configUri).build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        int status = response.getStatusCode();
        if (status != HttpStatus.OK.code()) {
            throw new IllegalStateException(
                    format("Request to '%s' returned unexpected status code: '%d'", configUri,
                            status));
        }
        return parseJson(response.getBody(), AccessControlRules.class);
    }

    public JsonAccessControl create()
    {
        return new JsonAccessControl(catalogName,
                this::restBasedRulesProvider);
    }
}
