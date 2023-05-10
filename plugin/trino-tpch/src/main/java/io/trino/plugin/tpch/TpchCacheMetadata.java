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
package io.trino.plugin.tpch;

import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;

public class TpchCacheMetadata
        implements ConnectorCacheMetadata
{
    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle table)
    {
        TpchTableHandle handle = (TpchTableHandle) table;
        if (!handle.getConstraint().isAll()) {
            // lossless conversion of TupleDomain to string requires JSON serialization
            return Optional.empty();
        }

        // ensure cache id generation is revisited whenever handle classes change
        handle = new TpchTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getScaleFactor(),
                handle.getConstraint());

        return Optional.of(new CacheTableId(handle.getSchemaName() + ":" + handle.getTableName() + ":" + handle.getScaleFactor()));
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        TpchColumnHandle handle = (TpchColumnHandle) column;

        // ensure cache id generation is revisited whenever handle classes change
        handle = new TpchColumnHandle(
                handle.getColumnName(),
                handle.getType());

        return Optional.of(new CacheColumnId(handle.getColumnName() + ":" + handle.getType()));
    }

    @Override
    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle handle)
    {
        return handle;
    }
}
