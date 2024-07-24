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
package io.trino.server.protocol;

import com.google.inject.Inject;
import io.trino.server.ServerConfig;
import io.trino.spi.QueryId;
import jakarta.ws.rs.core.UriInfo;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

public class QueryInfoUrlFactory
{
    private final Optional<String> queryInfoUrlTemplate;

    @Inject
    public QueryInfoUrlFactory(ServerConfig serverConfig)
    {
        this.queryInfoUrlTemplate = serverConfig.getQueryInfoUrlTemplate();

        // verify the template is a valid URL
        queryInfoUrlTemplate.ifPresent(template -> {
            try {
                new URI(template.replace("${QUERY_ID}", "query_id_value"));
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid query info URL template: " + template, e);
            }
        });
    }

    public Optional<URI> getQueryInfoUrl(QueryId queryId)
    {
        return queryInfoUrlTemplate
                .map(template -> template.replace("${QUERY_ID}", queryId.toString()))
                .map(URI::create);
    }

    public static URI getQueryInfoUri(Optional<URI> queryInfoUrl, QueryId queryId, UriInfo uriInfo)
    {
        return queryInfoUrl.orElseGet(() ->
                uriInfo.getRequestUriBuilder()
                        .replacePath("ui/query.html")
                        .replaceQuery(queryId.toString())
                        .build());
    }
}
