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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class QualifiedFunctionName
{
    private final Optional<String> catalogName;
    private final Optional<String> schemaName;
    private final String functionName;

    public static QualifiedFunctionName of(String functionName)
    {
        return new QualifiedFunctionName(Optional.empty(), Optional.empty(), functionName);
    }

    public static QualifiedFunctionName of(String schemaName, String functionName)
    {
        return new QualifiedFunctionName(Optional.empty(), Optional.of(schemaName), functionName);
    }

    public static QualifiedFunctionName of(String catalogName, String schemaName, String functionName)
    {
        return new QualifiedFunctionName(Optional.of(catalogName), Optional.of(schemaName), functionName);
    }

    private QualifiedFunctionName(Optional<String> catalogName, Optional<String> schemaName, String functionName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        catalogName.ifPresent(name -> checkArgument(!name.isEmpty(), "catalogName is empty"));
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        schemaName.ifPresent(name -> checkArgument(!name.isEmpty(), "schemaName is empty"));
        checkArgument(catalogName.isEmpty() || schemaName.isPresent(), "Schema name must be provided when catalog name is provided");
        this.functionName = requireNonNull(functionName, "functionName is null");
        checkArgument(!functionName.isEmpty(), "functionName is empty");
    }

    public Optional<String> getCatalogName()
    {
        return catalogName;
    }

    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    public String getFunctionName()
    {
        return functionName;
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
        QualifiedFunctionName that = (QualifiedFunctionName) o;
        return catalogName.equals(that.catalogName) &&
                schemaName.equals(that.schemaName) &&
                functionName.equals(that.functionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, functionName);
    }

    @Override
    public String toString()
    {
        return catalogName.map(name -> name + ".").orElse("") +
                schemaName.map(name -> name + ".").orElse("") +
                functionName;
    }
}
