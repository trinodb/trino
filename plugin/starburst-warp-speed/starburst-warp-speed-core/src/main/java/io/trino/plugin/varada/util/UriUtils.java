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
package io.trino.plugin.varada.util;

import io.airlift.http.client.HttpUriBuilder;
import io.trino.spi.Node;

import java.net.URI;

public abstract class UriUtils
{
    private UriUtils() {}

    @SuppressWarnings({"DeprecatedApi", "deprecation"})
    public static URI getHttpUri(Node node)
    {
        String schema = node.getHostAndPort().getPortOrDefault(8080) == 443 ? "https" : "http"; // UGLY UGLY UGLY - node URI is not available since trino 434
        HttpUriBuilder httpUriBuilder = HttpUriBuilder.uriBuilder().scheme(schema).host(node.getHost());
        if (node.getHostAndPort().hasPort()) {
            httpUriBuilder.port(node.getHostAndPort().getPort());
        }
        return httpUriBuilder.build();
    }
}
