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

import io.trino.filesystem.TrinoOutputStream;
import io.trino.spi.classloader.ThreadContextClassLoader;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

/**
 * Output stream of a native file system, used with the context class loader of the native file system.
 */
final class NativeHadoopOutputStream
        extends OutputStream
{
    private final TrinoOutputStream delegate;
    private final ClassLoader nativeClassLoader;

    NativeHadoopOutputStream(TrinoOutputStream delegate, ClassLoader nativeClassLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.nativeClassLoader = requireNonNull(nativeClassLoader, "nativeClassLoader is null");
    }

    @Override
    public void write(int b)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            delegate.write(b);
        }
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            delegate.write(buffer, offset, length);
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            delegate.flush();
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
