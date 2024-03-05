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

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;

@JsonInclude(NON_NULL)
public record OpaQueryInputResource(
        TrinoUser user,
        NamedEntity systemSessionProperty,
        TrinoCatalogSessionProperty catalogSessionProperty,
        TrinoFunction function,
        NamedEntity catalog,
        TrinoSchema schema,
        TrinoTable table,
        TrinoColumn column)
{
    public record NamedEntity(String name)
    {
        public NamedEntity
        {
            requireNonNull(name, "name is null");
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private TrinoUser user;
        private NamedEntity systemSessionProperty;
        private TrinoCatalogSessionProperty catalogSessionProperty;
        private NamedEntity catalog;
        private TrinoSchema schema;
        private TrinoTable table;
        private TrinoFunction function;
        private TrinoColumn column;

        private Builder() {}

        public Builder user(TrinoUser user)
        {
            this.user = user;
            return this;
        }

        public Builder systemSessionProperty(String systemSessionProperty)
        {
            this.systemSessionProperty = new NamedEntity(systemSessionProperty);
            return this;
        }

        public Builder catalogSessionProperty(TrinoCatalogSessionProperty catalogSessionProperty)
        {
            this.catalogSessionProperty = catalogSessionProperty;
            return this;
        }

        public Builder catalog(String catalog)
        {
            this.catalog = new NamedEntity(catalog);
            return this;
        }

        public Builder schema(TrinoSchema schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder table(TrinoTable table)
        {
            this.table = table;
            return this;
        }

        public Builder function(TrinoFunction function)
        {
            this.function = function;
            return this;
        }

        public Builder function(String functionName)
        {
            this.function = new TrinoFunction(functionName);
            return this;
        }

        public Builder column(TrinoColumn column)
        {
            this.column = column;
            return this;
        }

        public OpaQueryInputResource build()
        {
            return new OpaQueryInputResource(
                    this.user,
                    this.systemSessionProperty,
                    this.catalogSessionProperty,
                    this.function,
                    this.catalog,
                    this.schema,
                    this.table,
                    this.column);
        }
    }
}
