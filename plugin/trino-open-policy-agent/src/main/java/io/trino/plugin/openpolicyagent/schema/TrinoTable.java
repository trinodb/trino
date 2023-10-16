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
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@JsonInclude(NON_NULL)
public record TrinoTable(
        @JsonUnwrapped TrinoSchema catalogSchema,
        String tableName,
        Set<String> columns)
{
    public static TrinoTable fromTrinoTable(CatalogSchemaTableName table)
    {
        return TrinoTable.Builder.fromTrinoTable(table).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends BaseSchemaBuilder<TrinoTable, Builder>
    {
        public String tableName;
        public Set<String> columns;

        public static Builder fromTrinoTable(CatalogSchemaTableName table)
        {
            return builder()
                    .catalogName(table.getCatalogName())
                    .schemaName(table.getSchemaTableName().getSchemaName())
                    .tableName(table.getSchemaTableName().getTableName());
        }

        private Builder() {}

        @Override
        protected Builder getInstance()
        {
            return this;
        }

        public Builder tableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder columns(Set<String> columns)
        {
            this.columns = ImmutableSet.copyOf(columns);
            return this;
        }

        @Override
        public TrinoTable build()
        {
            return new TrinoTable(
                    new TrinoSchema(this.catalogName, this.schemaName, this.properties),
                    this.tableName,
                    this.columns);
        }
    }
}
