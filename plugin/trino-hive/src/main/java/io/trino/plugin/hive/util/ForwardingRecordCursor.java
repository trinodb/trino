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
package io.trino.plugin.hive.util;

import io.airlift.slice.Slice;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

public abstract class ForwardingRecordCursor
        implements RecordCursor
{
    protected abstract RecordCursor delegate();

    @Override
    public long getCompletedBytes()
    {
        return delegate().getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate().getReadTimeNanos();
    }

    @Override
    public Type getType(int field)
    {
        return delegate().getType(field);
    }

    @Override
    public boolean advanceNextPosition()
    {
        return delegate().advanceNextPosition();
    }

    @Override
    public boolean getBoolean(int field)
    {
        return delegate().getBoolean(field);
    }

    @Override
    public long getLong(int field)
    {
        return delegate().getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        return delegate().getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        return delegate().getSlice(field);
    }

    @Override
    public Object getObject(int field)
    {
        return delegate().getObject(field);
    }

    @Override
    public boolean isNull(int field)
    {
        return delegate().isNull(field);
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate().getMemoryUsage();
    }

    @Override
    public void close()
    {
        delegate().close();
    }
}
