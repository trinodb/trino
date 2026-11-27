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
package io.trino.hdfs.nativefs;

import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.classloader.ThreadContextClassLoader;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class NativeHadoopInputStream
        extends FSInputStream
{
    private final TrinoInputStream delegate;
    private final ClassLoader nativeClassLoader;

    NativeHadoopInputStream(TrinoInputStream delegate, ClassLoader nativeClassLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.nativeClassLoader = requireNonNull(nativeClassLoader, "nativeClassLoader is null");
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            delegate.seek(position);
        }
    }

    @Override
    public long getPos()
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            return delegate.getPosition();
        }
    }

    @Override
    public boolean seekToNewSource(long targetPosition)
    {
        return false;
    }

    @Override
    public int read()
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            return delegate.read();
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            return delegate.read(buffer, offset, length);
        }
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            return delegate.skip(n);
        }
    }

    @Override
    public int available()
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            return delegate.available();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            delegate.close();
        }
    }
}
