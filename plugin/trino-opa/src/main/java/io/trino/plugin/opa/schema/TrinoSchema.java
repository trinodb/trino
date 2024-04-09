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
package io.trino.plugin.opa.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.CatalogSchemaName;

import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;

@JsonInclude(NON_NULL)
public record TrinoSchema(
        String catalogName,
        String schemaName,
        Map<String, Optional<Object>> properties)
{
    public TrinoSchema
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");
        if (properties != null) {
            properties = ImmutableMap.copyOf(properties);
        }
    }

    public TrinoSchema(CatalogSchemaName schema)
    {
        this(schema.getCatalogName(), schema.getSchemaName());
    }

    public TrinoSchema(String catalogName, String schemaName)
    {
        this(catalogName, schemaName, null);
    }

    public TrinoSchema withProperties(Map<String, Optional<Object>> newProperties)
    {
        return new TrinoSchema(catalogName, schemaName, newProperties);
    }
}
