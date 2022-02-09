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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static java.util.Objects.requireNonNull;

public class SchemaInitializer
        implements Consumer<QueryRunner>
{
    private final String schemaName;
    private final Map<String, String> schemaProperties;
    private final Iterable<TpchTable<?>> clonedTpchTables;

    private SchemaInitializer(String schemaName, Map<String, String> schemaProperties, Iterable<TpchTable<?>> tpchTablesToClone)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.schemaProperties = requireNonNull(schemaProperties, "schemaProperties is null");
        this.clonedTpchTables = requireNonNull(tpchTablesToClone, "tpchTablesToClone is null");
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public void accept(QueryRunner queryRunner)
    {
        String schemaProperties = this.schemaProperties.entrySet().stream()
                .map(entry -> entry.getKey() + " = " + entry.getValue())
                .collect(Collectors.joining(", ", " WITH ( ", " )"));
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName + (this.schemaProperties.size() > 0 ? schemaProperties : ""));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), clonedTpchTables);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String schemaName = "tpch";
        private Map<String, String> schemaProperties = ImmutableMap.of();
        private Iterable<TpchTable<?>> cloneTpchTables = ImmutableSet.of();

        public Builder withSchemaName(String schemaName)
        {
            this.schemaName = requireNonNull(schemaName, "schemaName is null");
            return this;
        }

        public Builder withSchemaProperties(Map<String, String> schemaProperties)
        {
            this.schemaProperties = requireNonNull(schemaProperties, "schemaProperties is null");
            return this;
        }

        public Builder withClonedTpchTables(Iterable<TpchTable<?>> tpchTables)
        {
            this.cloneTpchTables = ImmutableSet.copyOf(tpchTables);
            return this;
        }

        public SchemaInitializer build()
        {
            return new SchemaInitializer(schemaName, schemaProperties, cloneTpchTables);
        }
    }
}
