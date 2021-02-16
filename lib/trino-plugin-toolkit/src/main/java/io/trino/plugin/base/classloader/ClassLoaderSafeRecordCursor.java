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
package io.trino.plugin.base.classloader;

import io.airlift.slice.Slice;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeRecordCursor
        implements RecordCursor
{
    private final RecordCursor delegate;
    private final ClassLoader classLoader;

    @Inject
    public ClassLoaderSafeRecordCursor(@ForClassLoaderSafe RecordCursor delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public long getCompletedBytes()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getCompletedBytes();
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getReadTimeNanos();
        }
    }

    @Override
    public Type getType(int field)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getType(field);
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.advanceNextPosition();
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getBoolean(field);
        }
    }

    @Override
    public long getLong(int field)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getLong(field);
        }
    }

    @Override
    public double getDouble(int field)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getDouble(field);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSlice(field);
        }
    }

    @Override
    public Object getObject(int field)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getObject(field);
        }
    }

    @Override
    public boolean isNull(int field)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.isNull(field);
        }
    }

    @Override
    public void close()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.close();
        }
    }
}
