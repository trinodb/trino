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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.MetadataUtil.checkObjectName;
import static java.util.Objects.requireNonNull;

public record QualifiedObjectName(String catalogName, String schemaName, String objectName, Optional<Predicate<String>> predicate)
{
    private static final Pattern UNQUOTED_COMPONENT = Pattern.compile("[a-zA-Z0-9_]+");
    private static final String COMPONENT = UNQUOTED_COMPONENT.pattern() + "|\"([^\"]|\"\")*\"";
    private static final Pattern PATTERN = Pattern.compile("(?<catalog>" + COMPONENT + ")\\.(?<schema>" + COMPONENT + ")\\.(?<table>" + COMPONENT + ")");

    @JsonCreator
    public static QualifiedObjectName valueOf(String name)
    {
        requireNonNull(name, "name is null");
        Matcher matcher = PATTERN.matcher(name);
        checkArgument(matcher.matches(), "Invalid name %s", name);
        return new QualifiedObjectName(unquoteIfNeeded(matcher.group("catalog")), unquoteIfNeeded(matcher.group("schema")), unquoteIfNeeded(matcher.group("table")));
    }

    public QualifiedObjectName
    {
        checkObjectName(catalogName, schemaName, objectName, predicate);
    }

    public QualifiedObjectName(String catalogName, String schemaName, String objectName)
    {
        this(catalogName, schemaName, objectName, Optional.empty());
    }

    public QualifiedObjectName asResolvedQualifiedObjectName(Predicate<String> predicate)
    {
        return asResolvedQualifiedObjectName(Optional.of(predicate));
    }

    public QualifiedObjectName asResolvedQualifiedObjectName(Optional<Predicate<String>> predicate)
    {
        requireNonNull(predicate, "predicate is null");
        return new QualifiedObjectName(catalogName, schemaName, objectName, predicate);
    }

    public SchemaTableName asSchemaTableName()
    {
        return new SchemaTableName(schemaName, objectName);
    }

    public CatalogSchemaTableName asCatalogSchemaTableName()
    {
        return new CatalogSchemaTableName(catalogName, schemaName, objectName);
    }

    public SchemaRoutineName asSchemaRoutineName()
    {
        return new SchemaRoutineName(schemaName, objectName);
    }

    public CatalogSchemaRoutineName asCatalogSchemaRoutineName()
    {
        return new CatalogSchemaRoutineName(catalogName, schemaName, objectName);
    }

    public QualifiedTablePrefix asQualifiedTablePrefix()
    {
        return new QualifiedTablePrefix(catalogName, schemaName, objectName);
    }

    public SchemaFunctionName asSchemaFunctionName()
    {
        return new SchemaFunctionName(schemaName, objectName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return quoteIfNeeded(catalogName, Optional.empty()) + '.' + quoteIfNeeded(schemaName, predicate) + '.' + quoteIfNeeded(objectName, predicate);
    }

    public static Function<SchemaTableName, QualifiedObjectName> convertFromSchemaTableName(String catalogName)
    {
        return input -> new QualifiedObjectName(catalogName, input.getSchemaName(), input.getTableName());
    }

    private static String unquoteIfNeeded(String name)
    {
        if (name.isEmpty() || name.charAt(0) != '"') {
            return name;
        }
        checkArgument(name.charAt(name.length() - 1) == '"', "Invalid name: [%s]", name);
        return name.substring(1, name.length() - 1).replace("\"\"", "\"");
    }

    private static String quoteIfNeeded(String name, Optional<Predicate<String>> predicate)
    {
        if (UNQUOTED_COMPONENT.matcher(name).matches() && !predicate.map(p -> p.test(name)).orElse(false)) {
            return name;
        }
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, objectName);
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
        QualifiedObjectName other = (QualifiedObjectName) obj;
        return Objects.equals(this.catalogName, other.catalogName) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.objectName, other.objectName);
    }
}
