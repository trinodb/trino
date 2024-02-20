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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import static java.util.Objects.requireNonNull;

public class TableFunctionHandle
{
    private final CatalogHandle catalogHandle;
    private final ConnectorTableFunctionHandle functionHandle;
    private final ConnectorTransactionHandle transactionHandle;

    @JsonCreator
    public TableFunctionHandle(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("functionHandle") ConnectorTableFunctionHandle functionHandle,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
    }

    @JsonProperty
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @JsonProperty
    public ConnectorTableFunctionHandle getFunctionHandle()
    {
        return functionHandle;
    }

    @JsonProperty
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }
}
