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
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Name;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.SchemaTableName;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.util.NameUtil.createName;
import static java.util.Objects.requireNonNull;

@Immutable
public class QualifiedObjectName
{
    public static QualifiedObjectName valueOf(String name)
    {
        requireNonNull(name, "name is null");

        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(name));
        checkArgument(ids.size() == 3, "Invalid name %s", name);

        return new QualifiedObjectName(createName(ids.get(0)), createName(ids.get(1)), createName(ids.get(2)));
    }

    private final Name catalogName;
    private final Name schemaName;
    private final Name objectName;

    @JsonCreator
    public QualifiedObjectName(
            @JsonProperty("catalogName") Name catalogName,
            @JsonProperty("schemaName") Name schemaName,
            @JsonProperty("objectName") Name objectName)
    {
        this.catalogName = requireNonNull(catalogName, "CatalogName is null");
        this.schemaName = requireNonNull(schemaName, "SchemaName is null");
        this.objectName = requireNonNull(objectName, "ObjectName is null");
    }

    @JsonProperty
    public Name getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public Name getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public Name getObjectName()
    {
        return objectName;
    }

    public SchemaTableName asSchemaTableName()
    {
        return new SchemaTableName(schemaName.getLegacyName(), objectName.getLegacyName());
    }

    public CatalogSchemaTableName asCatalogSchemaTableName()
    {
        return new CatalogSchemaTableName(catalogName, new SchemaTableName(schemaName.getLegacyName(), objectName.getLegacyName()));
    }

    public QualifiedTablePrefix asQualifiedTablePrefix()
    {
        return new QualifiedTablePrefix(catalogName, schemaName, objectName);
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
        QualifiedObjectName o = (QualifiedObjectName) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(objectName, o.objectName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, objectName);
    }

    @Override
    public String toString()
    {
        return catalogName.getLegacyName() + '.' + schemaName.getLegacyName() + '.' + objectName.getLegacyName();
    }

    public static Function<SchemaTableName, QualifiedObjectName> convertFromSchemaTableName(Name catalogName)
    {
        return input -> new QualifiedObjectName(catalogName, createName(input.getSchemaName()), createName(input.getTableName()));
    }
}
