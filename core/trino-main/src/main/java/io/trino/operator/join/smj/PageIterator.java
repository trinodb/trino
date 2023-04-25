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
package io.trino.operator.join.smj;

import com.google.common.util.concurrent.SettableFuture;
import io.trino.spi.Page;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class PageIterator
        implements Iterator
{
    private Queue<Page> buffer;
    private Integer maxBufferSize;
    private Page part;

    private volatile boolean noMorePage;
    private volatile boolean closed;

    private volatile SettableFuture<Void> full;

    public PageIterator(Integer maxBufferSize)
    {
        this.buffer = new LinkedList<>();
        this.maxBufferSize = maxBufferSize;
        this.full = SettableFuture.create();
        this.full.set(null);
        this.part = null;
        this.noMorePage = false;
        this.closed = false;
    }

    public synchronized SettableFuture<Void> add(Page page)
    {
        if (closed) {
            return null;
        }
        buffer.offer(page);
        return checkAndResetFullFlag();
    }

    private synchronized SettableFuture<Void> checkAndResetFullFlag()
    {
        if (buffer.size() >= maxBufferSize) {
            if (full.isDone()) {
                full = SettableFuture.create();
            }
        }
        else {
            full.set(null);
        }
        return full;
    }

    public void noMorePage()
    {
        noMorePage = true;
    }

    @Override
    public boolean hasNext()
    {
        return (part != null || !buffer.isEmpty()) && !closed;
    }

    @Override
    public synchronized Page next()
    {
        if (part != null) {
            return part;
        }
        Page page = buffer.poll();
        checkAndResetFullFlag();
        return page;
    }

    public synchronized void close()
    {
        closed = true;
        buffer.clear();
        part = null;
        full.set(null);
    }

    public boolean isFinished()
    {
        return noMorePage && !hasNext();
    }

    public SettableFuture<Void> getFull()
    {
        return full;
    }

    public boolean isClosed()
    {
        return closed;
    }
}
