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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record CatalogVersion(String version)
{
    private static final int INSTANCE_SIZE = instanceSize(CatalogVersion.class);

    /**
     * Version of a catalog.
     */
    @JsonCreator
    public CatalogVersion
    {
        requireNonNull(version, "version is null");
        if (version.isEmpty()) {
            throw new IllegalArgumentException("version is empty");
        }
        for (int i = 0; i < version.length(); i++) {
            if (!isAllowedCharacter(version.charAt(i))) {
                throw new IllegalArgumentException("invalid version: " + version);
            }
        }
    }

    private static boolean isAllowedCharacter(char c)
    {
        return ('0' <= c && c <= '9') ||
               ('a' <= c && c <= 'z') ||
               c == '_' ||
               c == '-';
    }

    @JsonValue
    @Override
    public String toString()
    {
        return version;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(version);
    }
}
