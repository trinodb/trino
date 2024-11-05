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
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class DeferredIterable
        implements Iterable<List<Object>>
{
    private final Future<Iterable<List<Object>>> future;
    private final Callable<Void> cleanUpAction;

    public DeferredIterable(Future<Iterable<List<Object>>> future, Callable<Void> cleanUpAction)
    {
        this.future = future;
        this.cleanUpAction = cleanUpAction;
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        try {
            // blocks until the data has been downloaded and decoded
            return new CleanupIterator(future.get().iterator(), cleanUpAction);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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
