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
package io.trino.plugin.doris;

import com.google.common.base.Splitter;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.net.URLEncoder;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class DorisFeEndpoints
{
    private static final Splitter FE_NODES_SPLITTER = Splitter.on(',')
            .trimResults()
            .omitEmptyStrings();

    private DorisFeEndpoints() {}

    static List<FeEndpoint> getEndpoints(DorisConfig config)
    {
        List<FeEndpoint> endpoints = config.getFenodes()
                .stream()
                .flatMap(FE_NODES_SPLITTER::splitToStream)
                .map(DorisFeEndpoints::parseEndpoint)
                .sorted(Comparator.comparing(FeEndpoint::sortKey))
                .toList();
        if (endpoints.isEmpty()) {
            throw new TrinoException(CONFIGURATION_INVALID, "doris.fenodes must be set for Doris FE access");
        }
        return endpoints;
    }

    public static List<String> getHosts(DorisConfig config)
    {
        return getEndpoints(config).stream()
                .map(FeEndpoint::host)
                .distinct()
                .toList();
    }

    private static FeEndpoint parseEndpoint(String endpoint)
    {
        try {
            if (endpoint.contains("://")) {
                URI uri = URI.create(endpoint);
                String scheme = Optional.ofNullable(uri.getScheme())
                        .map(value -> value.toLowerCase(Locale.ENGLISH))
                        .orElseThrow(() -> invalidFenode(endpoint, "scheme is missing"));
                if (!scheme.equals("http") && !scheme.equals("https")) {
                    throw invalidFenode(endpoint, "scheme must be http or https");
                }
                if (uri.getHost() == null) {
                    throw invalidFenode(endpoint, "host is missing");
                }
                if (uri.getPort() < 0) {
                    throw invalidFenode(endpoint, "port is missing");
                }
                if ((uri.getRawPath() != null && !uri.getRawPath().isEmpty())
                        || uri.getRawQuery() != null
                        || uri.getRawFragment() != null
                        || uri.getUserInfo() != null) {
                    throw invalidFenode(endpoint, "must be in the form http[s]://host:port");
                }
                return new FeEndpoint(Optional.of(scheme), uri.getHost(), uri.getPort());
            }

            HostAddress address = HostAddress.fromString(endpoint);
            if (!address.hasPort()) {
                throw invalidFenode(endpoint, "port is missing");
            }
            return new FeEndpoint(Optional.empty(), address.getHostText(), address.getPort());
        }
        catch (IllegalArgumentException e) {
            throw invalidFenode(endpoint, e.getMessage());
        }
    }

    private static TrinoException invalidFenode(String endpoint, String details)
    {
        return new TrinoException(CONFIGURATION_INVALID, "Invalid Doris FE endpoint '%s': %s".formatted(endpoint, details));
    }

    record FeEndpoint(Optional<String> scheme, String host, int port)
    {
        FeEndpoint
        {
            requireNonNull(scheme, "scheme is null");
            requireNonNull(host, "host is null");
        }

        public String authority()
        {
            return HostAddress.fromParts(host, port).toString();
        }

        public List<URI> queryPlanUris(String schemaName, String tableName)
        {
            if (scheme.isPresent()) {
                return List.of(queryPlanUri(scheme.get(), schemaName, tableName));
            }
            return List.of(
                    queryPlanUri("http", schemaName, tableName),
                    queryPlanUri("https", schemaName, tableName));
        }

        public String displayName()
        {
            return scheme.map(value -> value + "://" + authority()).orElse(authority());
        }

        private URI queryPlanUri(String scheme, String schemaName, String tableName)
        {
            return URI.create("%s://%s/api/%s/%s/_query_plan".formatted(
                    scheme,
                    authority(),
                    encodePathSegment(schemaName),
                    encodePathSegment(tableName)));
        }

        private static String encodePathSegment(String value)
        {
            // URLEncoder is form-oriented and maps spaces to '+', so normalize to percent encoding for URI paths.
            return URLEncoder.encode(value, UTF_8).replace("+", "%20");
        }

        private String sortKey()
        {
            return displayName();
        }
    }
}
