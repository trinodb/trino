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

import com.google.common.collect.AbstractIterator;
import io.trino.spi.classloader.ThreadContextClassLoader;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public final class ClassLoaderSafeIterator<T>
        extends AbstractIterator<T>
{
    private final Iterator<T> delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeIterator(Iterator<T> delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    protected T computeNext()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (!delegate.hasNext()) {
                return endOfData();
            }
            return delegate.next();
        }
    }
}
