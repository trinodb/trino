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
package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.Name;
import io.prestosql.spi.connector.SchemaTablePrefix;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class QualifiedTablePrefix
{
    private final Name catalogName;
    private final Optional<Name> schemaName;
    private final Optional<Name> tableName;

    public QualifiedTablePrefix(Name catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "CatalogName is null");
        this.schemaName = Optional.empty();
        this.tableName = Optional.empty();
    }

    public QualifiedTablePrefix(Name catalogName, Name schemaName)
    {
        this.catalogName = requireNonNull(catalogName, "CatalogName is null");
        this.schemaName = Optional.of(requireNonNull(schemaName, "SchemaName is null"));
        this.tableName = Optional.empty();
    }

    public QualifiedTablePrefix(Name catalogName, Name schemaName, Name tableName)
    {
        this.catalogName = requireNonNull(catalogName, "CatalogName is null");
        this.schemaName = Optional.of(requireNonNull(schemaName, "SchemaName is null"));
        this.tableName = Optional.of(requireNonNull(tableName, "TableName is null"));
    }

    @JsonCreator
    public QualifiedTablePrefix(
            @JsonProperty("catalogName") Name catalogName,
            @JsonProperty("schemaName") Optional<Name> schemaName,
            @JsonProperty("tableName") Optional<Name> tableName)
    {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @JsonProperty
    public Name getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public Optional<Name> getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public Optional<Name> getTableName()
    {
        return tableName;
    }

    public boolean hasSchemaName()
    {
        return schemaName.isPresent();
    }

    public boolean hasTableName()
    {
        return tableName.isPresent();
    }

    public SchemaTablePrefix asSchemaTablePrefix()
    {
        if (!schemaName.isPresent()) {
            return new SchemaTablePrefix();
        }
        else if (!tableName.isPresent()) {
            return new SchemaTablePrefix(schemaName.get());
        }
        else {
            return new SchemaTablePrefix(schemaName.get(), tableName.get());
        }
    }

    public boolean matches(QualifiedObjectName objectName)
    {
        return Objects.equals(catalogName, objectName.getCatalogName())
                && schemaName.map(schema -> Objects.equals(schema, objectName.getSchemaName())).orElse(true)
                && tableName.map(table -> Objects.equals(table, objectName.getObjectName())).orElse(true);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QualifiedTablePrefix o = (QualifiedTablePrefix) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(tableName, o.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return catalogName.getLegacyName() + '.' + schemaName.map(Name::getLegacyName).orElse("*") + '.' + tableName.map(Name::getLegacyName).orElse("*");
    }
}
