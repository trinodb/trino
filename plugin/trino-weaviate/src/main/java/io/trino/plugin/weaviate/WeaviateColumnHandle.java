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
package io.trino.plugin.weaviate;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.VectorConfig;

import java.util.Map;

import static io.trino.plugin.weaviate.WeaviateUtil.VECTORS_COLUMN_NAME;
import static io.trino.plugin.weaviate.WeaviateUtil.toTrinoType;
import static java.util.Objects.requireNonNull;

public record WeaviateColumnHandle(String name, Type trinoType, boolean hidden)
        implements ColumnHandle
{
    public WeaviateColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(trinoType, "trinoType is null");
    }

    public WeaviateColumnHandle(Property property)
    {
        this(property, false);
    }

    public WeaviateColumnHandle(Property property, boolean hidden)
    {
        this(property.propertyName(), toTrinoType(property), hidden);
    }

    public WeaviateColumnHandle(Map<String, VectorConfig> vectors)
    {
        this(VECTORS_COLUMN_NAME, toTrinoType(vectors), true);
    }

    public ColumnMetadata columnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(trinoType)
                .setHidden(hidden)
                .build();
    }
}
