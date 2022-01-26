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
package io.trino.plugin.deltalake.util;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Like {@link com.google.common.io.Closer}, but additionally adds support for {@link AutoCloseable},
 * and hides {@code UnstableApiUsage} warnings.
 */
@SuppressWarnings("UnstableApiUsage")
final class Closer
        implements Closeable
{
    private final com.google.common.io.Closer closer;

    private Closer(com.google.common.io.Closer closer)
    {
        this.closer = closer;
    }

    public static Closer create()
    {
        return new Closer(com.google.common.io.Closer.create());
    }

    @CanIgnoreReturnValue
    public <C extends Closeable> C register(@Nullable C closeable)
    {
        return closer.register(closeable);
    }

    @CanIgnoreReturnValue
    public <C extends AutoCloseable> C register(C closeable)
    {
        closer.register(new Closeable()
        {
            private boolean closed;

            @Override
            public void close()
                    throws IOException
            {
                if (!closed) {
                    try {
                        closeable.close();
                    }
                    catch (Exception e) {
                        throwIfUnchecked(e);
                        propagateIfPossible(e, IOException.class);
                        throw new RuntimeException(e);
                    }
                    closed = true;
                }
            }
        });

        return closeable;
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
