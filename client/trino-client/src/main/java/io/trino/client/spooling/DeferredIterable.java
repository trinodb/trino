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
package io.trino.client.spooling;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DeferredIterable
        implements Iterable<List<Object>>
{
    private final Future<Iterable<List<Object>>> futureIterable;
    private final long size;
    private final Callable<Void> cleanUpAction;

    public DeferredIterable(Future<Iterable<List<Object>>> futureIterable, long size, Callable<Void> cleanUpAction)
    {
        this.futureIterable = futureIterable;
        this.size = size;
        this.cleanUpAction = cleanUpAction;
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return new CleanupIterator(new Iterator<List<Object>>()
        {
            private Iterator<List<Object>> actualIterator;
            private int currentIndex;

            private void initializeIterator()
            {
                if (actualIterator == null) {
                    try {
                        Iterable<List<Object>> iterable = futureIterable.get();
                        actualIterator = iterable.iterator();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException("Error while retrieving the future", e);
                    }
                }
            }

            @Override
            public boolean hasNext()
            {
                return currentIndex < size;
            }

            @Override
            public List<Object> next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more elements available");
                }
                currentIndex++;

                // Return a lazy proxy for the actual list
                return new LazyListProxy();
            }

            // Lazy proxy for List<Object>
            class LazyListProxy
                    implements List<Object>
            {
                private List<Object> actualList;

                private void initializeList()
                {
                    if (actualList == null) {
                        initializeIterator();
                        if (!actualIterator.hasNext()) {
                            throw new IllegalStateException("Underlying data has fewer elements than expected");
                        }
                        actualList = actualIterator.next();
                    }
                }

                @Override
                public int size()
                {
                    initializeList();
                    return actualList.size();
                }

                @Override
                public boolean isEmpty()
                {
                    initializeList();
                    return actualList.isEmpty();
                }

                @Override
                public boolean contains(Object o)
                {
                    initializeList();
                    return actualList.contains(o);
                }

                @Override
                public Iterator<Object> iterator()
                {
                    initializeList();
                    return actualList.iterator();
                }

                @Override
                public Object[] toArray()
                {
                    initializeList();
                    return actualList.toArray();
                }

                @Override
                public <T> T[] toArray(T[] a)
                {
                    initializeList();
                    return actualList.toArray(a);
                }

                @Override
                public boolean add(Object o)
                {
                    initializeList();
                    return actualList.add(o);
                }

                @Override
                public boolean remove(Object o)
                {
                    initializeList();
                    return actualList.remove(o);
                }

                @Override
                public boolean containsAll(java.util.Collection<?> c)
                {
                    initializeList();
                    return actualList.containsAll(c);
                }

                @Override
                public boolean addAll(java.util.Collection<? extends Object> c)
                {
                    initializeList();
                    return actualList.addAll(c);
                }

                @Override
                public boolean addAll(int index, java.util.Collection<? extends Object> c)
                {
                    initializeList();
                    return actualList.addAll(index, c);
                }

                @Override
                public boolean removeAll(java.util.Collection<?> c)
                {
                    initializeList();
                    return actualList.removeAll(c);
                }

                @Override
                public boolean retainAll(java.util.Collection<?> c)
                {
                    initializeList();
                    return actualList.retainAll(c);
                }

                @Override
                public void clear()
                {
                    initializeList();
                    actualList.clear();
                }

                @Override
                public Object get(int index)
                {
                    initializeList();
                    return actualList.get(index);
                }

                @Override
                public Object set(int index, Object element)
                {
                    initializeList();
                    return actualList.set(index, element);
                }

                @Override
                public void add(int index, Object element)
                {
                    initializeList();
                    actualList.add(index, element);
                }

                @Override
                public Object remove(int index)
                {
                    initializeList();
                    return actualList.remove(index);
                }

                @Override
                public int indexOf(Object o)
                {
                    initializeList();
                    return actualList.indexOf(o);
                }

                @Override
                public int lastIndexOf(Object o)
                {
                    initializeList();
                    return actualList.lastIndexOf(o);
                }

                @Override
                public ListIterator<Object> listIterator()
                {
                    initializeList();
                    return actualList.listIterator();
                }

                @Override
                public ListIterator<Object> listIterator(int index)
                {
                    initializeList();
                    return actualList.listIterator(index);
                }

                @Override
                public List<Object> subList(int fromIndex, int toIndex)
                {
                    initializeList();
                    return actualList.subList(fromIndex, toIndex);
                }
            }
        }, cleanUpAction);
    }

    private static class CleanupIterator
            implements Iterator<List<Object>>
    {
        private final Iterator<List<Object>> delegate;
        private boolean isDone;
        private final Callable<Void> cleanUpAction;

        public CleanupIterator(Iterator<List<Object>> delegate, Callable<Void> cleanUpAction)
        {
            this.delegate = delegate;
            this.cleanUpAction = cleanUpAction;
        }

        @Override
        public boolean hasNext()
        {
            boolean hasNext = delegate.hasNext();
            if (!hasNext) {
                cleanup();
            }
            return hasNext;
        }

        @Override
        public List<Object> next()
        {
            return delegate.next();
        }

        @Override
        public void remove()
        {
            delegate.remove();
        }

        private void cleanup()
        {
            if (!isDone) {
                isDone = true;
                try {
                    cleanUpAction.call();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
