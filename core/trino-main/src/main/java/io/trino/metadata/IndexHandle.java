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

import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public record IndexHandle(
        CatalogHandle catalogHandle,
        ConnectorTransactionHandle transactionHandle,
        ConnectorIndexHandle connectorHandle)
{
    public IndexHandle
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(transactionHandle, "transactionHandle is null");
        requireNonNull(connectorHandle, "connectorHandle is null");
    }

    @Override
    public String toString()
    {
        return catalogHandle + ":" + connectorHandle;
    }
}
