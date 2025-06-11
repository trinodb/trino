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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class NodeVersion
{
    public static final NodeVersion UNKNOWN = new NodeVersion("<unknown>", Optional.empty());

    private final String version;
    private final Optional<String> javaVersion;

    @JsonCreator
    public NodeVersion(@JsonProperty("version") String version, @JsonProperty("java_version") Optional<String> javaVersion)
    {
        this.version = requireNonNull(version, "version is null");
        this.javaVersion = requireNonNull(javaVersion, "javaVersion is null");
    }

    public NodeVersion(String version)
    {
        this(version, Optional.empty());
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @JsonProperty
    public Optional<String> getJavaVersion()
    {
        return javaVersion;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodeVersion that = (NodeVersion) o;
        return Objects.equals(version, that.version) &&
               Objects.equals(javaVersion, that.javaVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, javaVersion);
    }

    @Override
    public String toString()
    {
        return version + " (" + javaVersion.orElse("unknown") + ")";
    }
}
