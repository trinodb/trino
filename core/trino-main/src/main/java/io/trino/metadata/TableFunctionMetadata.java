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

import io.trino.connector.CatalogName;
import io.trino.spi.ptf.ConnectorTableFunction;

import static java.util.Objects.requireNonNull;

public class TableFunctionMetadata
{
    private final CatalogName catalogName;
    private final ConnectorTableFunction function;

    public TableFunctionMetadata(CatalogName catalogName, ConnectorTableFunction function)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.function = requireNonNull(function, "function is null");
    }

    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    public ConnectorTableFunction getFunction()
    {
        return function;
    }
}
