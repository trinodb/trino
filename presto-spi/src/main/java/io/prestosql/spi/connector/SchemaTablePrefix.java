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
package io.prestosql.spi.connector;

import java.util.Objects;
import java.util.Optional;

import static io.prestosql.spi.connector.SchemaUtil.checkNotEmpty;

public class SchemaTablePrefix
{
    private final Optional<String> schemaName;
    private final Optional<String> tableName;

    public SchemaTablePrefix()
    {
        this.schemaName = Optional.empty();
        this.tableName = Optional.empty();
    }

    public SchemaTablePrefix(String schemaName)
    {
        this.schemaName = Optional.of(checkNotEmpty(schemaName, "schemaName"));
        this.tableName = Optional.empty();
    }

    public SchemaTablePrefix(String schemaName, String tableName)
    {
        this.schemaName = Optional.of(checkNotEmpty(schemaName, "schemaName"));
        this.tableName = Optional.of(checkNotEmpty(tableName, "tableName"));
    }

    public Optional<String> getSchema()
    {
        return schemaName;
    }

    public Optional<String> getTable()
    {
        return tableName;
    }

    public boolean matches(SchemaTableName schemaTableName)
    {
        // empty prefix matches everything
        if (isEmpty()) {
            return true;
        }

        if (!schemaName.get().equals(schemaTableName.getSchemaName())) {
            return false;
        }

        return !tableName.isPresent() || tableName.get().equals(schemaTableName.getTableName());
    }

    public boolean isEmpty()
    {
        return !schemaName.isPresent();
    }

    public SchemaTableName toSchemaTableName()
    {
        return toOptionalSchemaTableName()
                .orElseThrow(() -> new IllegalStateException("both schemaName and tableName must be set"));
    }

    public Optional<SchemaTableName> toOptionalSchemaTableName()
    {
        if (schemaName.isPresent() && tableName.isPresent()) {
            return Optional.of(new SchemaTableName(schemaName.get(), tableName.get()));
        }
        return Optional.empty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SchemaTablePrefix other = (SchemaTablePrefix) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return schemaName.orElse("*") + '.' + tableName.orElse("*");
    }
}
