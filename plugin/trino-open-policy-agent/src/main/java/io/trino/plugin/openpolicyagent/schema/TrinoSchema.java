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
package io.trino.plugin.openpolicyagent.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.trino.spi.connector.CatalogSchemaName;

import java.util.Map;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TrinoSchema(String catalogName,
                          String schemaName,
                          Map<String, Optional<Object>> properties)
{
    public static class Builder
            extends BaseSchemaBuilder<TrinoSchema, Builder>
    {
        @Override
        protected Builder getInstance()
        {
            return this;
        }

        public static Builder fromTrinoCatalogSchema(CatalogSchemaName catalogSchemaName)
        {
            return new Builder()
                    .catalogName(catalogSchemaName.getCatalogName())
                    .schemaName(catalogSchemaName.getSchemaName());
        }

        @Override
        public TrinoSchema build()
        {
            return new TrinoSchema(this);
        }
    }

    public static TrinoSchema fromTrinoCatalogSchema(CatalogSchemaName catalogSchemaName)
    {
        return Builder.fromTrinoCatalogSchema(catalogSchemaName).build();
    }

    public <T extends BaseSchemaBuilder<?, T>> TrinoSchema(T builder)
    {
        this(builder.catalogName, builder.schemaName, builder.properties);
    }
}
