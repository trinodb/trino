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
package io.trino.spi.expression;

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public final class Call
        extends ConnectorExpression
{
    private final Optional<CatalogSchemaName> catalogSchemaName;
    private final String name;
    private final List<ConnectorExpression> arguments;

    public Call(
            Type type,
            Optional<CatalogSchemaName> catalogSchemaName,
            String name,
            List<ConnectorExpression> arguments)
    {
        super(type);
        this.catalogSchemaName = requireNonNull(catalogSchemaName, "catalogSchemaName is null");
        this.name = requireNonNull(name, "name is null");
        this.arguments = List.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    /**
     * @return the catalog and schema of this function call, or {@link Optional#empty()} if this is a built-in function call
     */
    public Optional<CatalogSchemaName> getCatalogSchemaName()
    {
        return catalogSchemaName;
    }

    /**
     * @return the function's name
     */
    public String getName()
    {
        return name;
    }

    public List<ConnectorExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return arguments;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Call call = (Call) o;
        return Objects.equals(catalogSchemaName, call.catalogSchemaName) &&
                Objects.equals(name, call.name) &&
                Objects.equals(arguments, call.arguments) &&
                Objects.equals(getType(), call.getType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogSchemaName, name, arguments, getType());
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", Call.class.getSimpleName() + "[", "]");
        catalogSchemaName.ifPresent(name -> stringJoiner.add("catalogSchemaName=" + name));
        return stringJoiner
                .add("name='" + name + "'")
                .add("arguments=" + arguments)
                .toString();
    }
}
