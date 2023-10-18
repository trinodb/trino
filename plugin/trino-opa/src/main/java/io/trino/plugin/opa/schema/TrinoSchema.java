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
import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;

@JsonInclude(NON_NULL)
public record TrinoSchema(
        @NotNull String catalogName,
        @NotNull String schemaName,
        Map<String, Optional<Object>> properties)
{
    public static TrinoSchema fromTrinoCatalogSchema(CatalogSchemaName catalogSchemaName)
    {
        return Builder.fromTrinoCatalogSchema(catalogSchemaName).build();
    }

    public TrinoSchema
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");
        if (properties != null) {
            properties = ImmutableMap.copyOf(properties);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends BaseSchemaBuilder<TrinoSchema, Builder>
    {
        public static Builder fromTrinoCatalogSchema(CatalogSchemaName catalogSchemaName)
        {
            return new Builder()
                    .catalogName(catalogSchemaName.getCatalogName())
                    .schemaName(catalogSchemaName.getSchemaName());
        }

        private Builder() {}

        @Override
        protected Builder getInstance()
        {
            return this;
        }

        @Override
        public TrinoSchema build()
        {
            return new TrinoSchema(
                    this.catalogName,
                    this.schemaName,
                    this.properties);
        }
    }
}
