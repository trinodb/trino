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
package io.trino.spi.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

public final class CatalogName
{
    private static final int INSTANCE_SIZE = instanceSize(CatalogName.class);

    private final String catalogName;

    @JsonCreator
    public CatalogName(String catalogName)
    {
        if (catalogName.isEmpty()) {
            throw new IllegalArgumentException("catalogName is empty");
        }
        this.catalogName = catalogName;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(catalogName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof CatalogName other &&
                catalogName.equals(other.catalogName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName);
    }
}
