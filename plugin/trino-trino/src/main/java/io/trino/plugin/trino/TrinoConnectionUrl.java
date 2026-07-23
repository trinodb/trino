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
package io.trino.plugin.trino;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNullElse;

final class TrinoConnectionUrl
{
    private static final String JDBC_PREFIX = "jdbc:trino://";

    private TrinoConnectionUrl() {}

    static void validate(String connectionUrl)
    {
        List<String> pathSegments = parsePathSegments(connectionUrl);
        if (pathSegments.isEmpty()) {
            throw new IllegalArgumentException("connection-url must include a remote catalog in the path: jdbc:trino://host:port/catalog[/schema]");
        }
        if (pathSegments.size() > 2) {
            throw new IllegalArgumentException("connection-url may include only a remote catalog and optional schema: jdbc:trino://host:port/catalog[/schema]");
        }
    }

    private static List<String> parsePathSegments(String connectionUrl)
    {
        if (connectionUrl == null || !connectionUrl.startsWith(JDBC_PREFIX)) {
            throw new IllegalArgumentException("connection-url must be a Trino JDBC URL with a remote catalog: jdbc:trino://host:port/catalog[/schema]");
        }
        URI uri = URI.create("trino://" + connectionUrl.substring(JDBC_PREFIX.length()));
        return Arrays.stream(requireNonNullElse(uri.getPath(), "").split("/"))
                .filter(segment -> !segment.isEmpty())
                .toList();
    }
}
