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
package io.trino.plugin.ldapgroup;

import javax.naming.NamingEnumeration;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class TestingNamingEnumeration<T>
        implements NamingEnumeration<T>
{
    private final Iterator<T> iterator;

    private TestingNamingEnumeration(List<T> items)
    {
        this.iterator = items.iterator();
    }

    private TestingNamingEnumeration()
    {
        this.iterator = Collections.emptyIterator();
    }

    public static <V> TestingNamingEnumeration<V> of()
    {
        return new TestingNamingEnumeration<>();
    }

    @SafeVarargs
    public static <V> TestingNamingEnumeration<V> of(V... values)
    {
        return new TestingNamingEnumeration<>(Arrays.stream(values).toList());
    }

    @Override
    public T next()
    {
        return iterator.next();
    }

    @Override
    public boolean hasMore()
    {
        return iterator.hasNext();
    }

    @Override
    public void close()
    {}

    @Override
    public boolean hasMoreElements()
    {
        return iterator.hasNext();
    }

    @Override
    public T nextElement()
    {
        return iterator.next();
    }
}
