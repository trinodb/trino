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
package io.trino.spi.security;

import io.trino.spi.connector.CatalogSchemaName;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ViewExpression
{
    private final Optional<String> identity;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final String expression;
    private final List<CatalogSchemaName> path;

    private ViewExpression(Optional<String> identity, Optional<String> catalog, Optional<String> schema, String expression, List<CatalogSchemaName> path)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.path = List.copyOf(path);

        if (catalog.isEmpty() && schema.isPresent()) {
            throw new IllegalArgumentException("catalog must be present if schema is present");
        }
    }

    /**
     * @return user as whom the view expression will be evaluated. If empty identity is returned,
     * then session user is used.
     */
    public Optional<String> getSecurityIdentity()
    {
        return identity;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public String getExpression()
    {
        return expression;
    }

    public List<CatalogSchemaName> getPath()
    {
        return path;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String identity;
        private String catalog;
        private String schema;
        private String expression;
        private List<CatalogSchemaName> path = List.of();

        private Builder() {}

        public Builder identity(String identity)
        {
            this.identity = identity;
            return this;
        }

        public Builder catalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public Builder schema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder expression(String expression)
        {
            this.expression = expression;
            return this;
        }

        public void setPath(List<CatalogSchemaName> path)
        {
            this.path = List.copyOf(path);
        }

        public ViewExpression build()
        {
            return new ViewExpression(
                    Optional.ofNullable(identity),
                    Optional.ofNullable(catalog),
                    Optional.ofNullable(schema),
                    expression,
                    path);
        }
    }
}
