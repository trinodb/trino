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
package io.trino.execution;

import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ColumnInfo
{
    private final Optional<CatalogSchemaTableName> schemaTable;
    private final String name;
    private final Optional<String> label;
    private final boolean autoIncrement;
    private final boolean nullable;
    private final boolean readOnly;

    public ColumnInfo(String name)
    {
        this(Optional.empty(), name, Optional.empty(), false, true, true);
    }

    public ColumnInfo(Optional<CatalogSchemaTableName> schemaTable, String name, Optional<String> label, boolean autoIncrement, boolean nullable, boolean readOnly)
    {
        this.schemaTable = requireNonNull(schemaTable, "schemaTable is null");
        this.name = requireNonNull(name, "name is null");
        this.label = requireNonNull(label, "label is null");
        this.autoIncrement = autoIncrement;
        this.nullable = nullable;
        this.readOnly = readOnly;
    }

    public Optional<String> getCatalogName()
    {
        return schemaTable.map(CatalogSchemaTableName::getCatalogName);
    }

    public Optional<String> getSchemaName()
    {
        return schemaTable.map(e -> e.getSchemaTableName().getSchemaName());
    }

    public Optional<String> getTableName()
    {
        return schemaTable.map(e -> e.getSchemaTableName().getTableName());
    }

    public String getName()
    {
        return name;
    }

    public Optional<String> getLabel()
    {
        return label;
    }

    public boolean isAutoIncrement()
    {
        return autoIncrement;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<CatalogSchemaTableName> schemaTable = Optional.empty();
        private String name;
        private Optional<String> label = Optional.empty();
        private boolean autoIncrement;
        private boolean nullable = true;
        private boolean readOnly;

        public Builder() {}

        public Builder setSchemaTable(Optional<QualifiedObjectName> objectName)
        {
            this.schemaTable = objectName.flatMap(o -> Optional.of(o.asCatalogSchemaTableName()));
            return this;
        }

        public void setMetadata(ColumnMetadata metadata)
        {
            this.autoIncrement = metadata.isAutoIncrement();
            this.nullable = metadata.isNullable();
            this.readOnly = metadata.isReadOnly();
        }

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setLabel(Optional<String> label)
        {
            this.label = label;
            return this;
        }

        public Builder setAutoIncrement(boolean autoIncrement)
        {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder setNullable(boolean nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public Builder setReadOnly(boolean readOnly)
        {
            this.readOnly = readOnly;
            return this;
        }

        public ColumnInfo build()
        {
            return new ColumnInfo(
                    schemaTable,
                    name,
                    label,
                    autoIncrement,
                    nullable,
                    readOnly);
        }
    }
}
