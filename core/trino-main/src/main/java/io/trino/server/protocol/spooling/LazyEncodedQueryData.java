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
package io.trino.server.protocol.spooling;

import io.trino.client.QueryData;
import io.trino.client.spooling.EncodedQueryData;

import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Objects.requireNonNull;

/**
 * Defers storage signing until response serialization, outside the query monitor.
 * Successful results are shared by concurrent serializers and response retries.
 */
final class LazyEncodedQueryData
        implements QueryData
{
    private final long rowsCount;
    private final Supplier<EncodedQueryData> data;

    public LazyEncodedQueryData(long rowsCount, Supplier<EncodedQueryData> data)
    {
        this.rowsCount = rowsCount;
        this.data = memoize(requireNonNull(data, "data is null")::get);
    }

    public EncodedQueryData data()
    {
        return data.get();
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public long getRowsCount()
    {
        return rowsCount;
    }
}
