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
package io.prestosql.jdbc;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

enum ClientInfoProperty
{
    APPLICATION_NAME("ApplicationName"),
    CLIENT_INFO("ClientInfo"),
    CLIENT_TAGS("ClientTags"),
    TRACE_TOKEN("TraceToken"),
    /**/;

    private static final Map<String, ClientInfoProperty> BY_NAME = Stream.of(values())
            .collect(toImmutableMap(ClientInfoProperty::getPropertyName, identity()));

    private final String propertyName;

    ClientInfoProperty(String propertyName)
    {
        this.propertyName = requireNonNull(propertyName, "propertyName is null");
    }

    public String getPropertyName()
    {
        return propertyName;
    }

    public static Optional<ClientInfoProperty> forName(String propertyName)
    {
        requireNonNull(propertyName, "propertyName is null");
        return Optional.ofNullable(BY_NAME.get(propertyName));
    }
}
