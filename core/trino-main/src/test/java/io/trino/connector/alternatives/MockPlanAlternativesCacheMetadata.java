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
package io.trino.connector.alternatives;

import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MockPlanAlternativesCacheMetadata
        implements ConnectorCacheMetadata
{
    private final ConnectorCacheMetadata delegate;

    public MockPlanAlternativesCacheMetadata(ConnectorCacheMetadata delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
    {
        return delegate.getCacheTableId(getDelegate(tableHandle));
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return delegate.getCacheColumnId(getDelegate(tableHandle), columnHandle);
    }

    @Override
    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle handle)
    {
        ConnectorTableHandle delegateResult = delegate.getCanonicalTableHandle(getDelegate(handle));
        return withDelegate(handle, delegateResult);
    }

    private ConnectorTableHandle getDelegate(ConnectorTableHandle tableHandle)
    {
        return tableHandle instanceof MockPlanAlternativeTableHandle handle ? handle.delegate() : tableHandle;
    }

    private ConnectorTableHandle withDelegate(ConnectorTableHandle tableHandle, ConnectorTableHandle delegate)
    {
        if (tableHandle instanceof MockPlanAlternativeTableHandle handle) {
            return new MockPlanAlternativeTableHandle(delegate, handle.filterColumn(), handle.filterDefinition());
        }
        return delegate;
    }
}
