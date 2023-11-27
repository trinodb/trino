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
package io.trino.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.LanguageFunctionManager;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.PathElement;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static java.util.Objects.requireNonNull;

public final class SqlPath
{
    public static final SqlPath EMPTY_PATH = buildPath("", Optional.empty());

    private final List<CatalogSchemaName> path;
    private final String rawPath;

    public static SqlPath buildPath(String rawPath, Optional<String> defaultCatalog)
    {
        ImmutableList.Builder<CatalogSchemaName> path = ImmutableList.builder();
        path.add(new CatalogSchemaName(GlobalSystemConnector.NAME, LanguageFunctionManager.QUERY_LOCAL_SCHEMA));
        path.add(new CatalogSchemaName(GlobalSystemConnector.NAME, GlobalFunctionCatalog.BUILTIN_SCHEMA));
        for (SqlPathElement pathElement : parsePath(rawPath)) {
            pathElement.getCatalog()
                    .map(Identifier::getValue).or(() -> defaultCatalog)
                    .ifPresent(catalog -> path.add(new CatalogSchemaName(catalog, pathElement.getSchema().getValue())));
        }
        return new SqlPath(path.build(), rawPath);
    }

    @JsonCreator
    public SqlPath(@JsonProperty List<CatalogSchemaName> path, @JsonProperty String rawPath)
    {
        this.path = ImmutableList.copyOf(path);
        this.rawPath = requireNonNull(rawPath, "rawPath is null");
    }

    @JsonProperty
    public String getRawPath()
    {
        return rawPath;
    }

    @JsonProperty
    public List<CatalogSchemaName> getPath()
    {
        return path;
    }

    public static List<SqlPathElement> parsePath(String rawPath)
    {
        if (rawPath.isBlank()) {
            return ImmutableList.of();
        }

        SqlParser parser = new SqlParser();
        List<PathElement> pathSpecification = parser.createPathSpecification(rawPath).getPath();

        List<SqlPathElement> pathElements = pathSpecification.stream()
                .map(pathElement -> new SqlPathElement(pathElement.getCatalog(), pathElement.getSchema()))
                .collect(toImmutableList());
        return pathElements;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SqlPath that = (SqlPath) obj;
        return Objects.equals(path, that.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path);
    }

    @Override
    public String toString()
    {
        return rawPath;
    }

    public SqlPath forView(List<CatalogSchemaName> storedPath)
    {
        // For a view, we prepend the global function schema to the path, as the
        // global function schema should not be in the path that is stored for the view.
        // We do not change the raw path, as that is used for the current_path function.
        List<CatalogSchemaName> viewPath = ImmutableList.<CatalogSchemaName>builder()
                .add(new CatalogSchemaName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA))
                .addAll(storedPath)
                .build();
        return new SqlPath(viewPath, rawPath);
    }
}
