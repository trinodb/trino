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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.util.LockUtils.CloseableLock;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.util.LockUtils.closeable;
import static java.util.Objects.requireNonNull;

public class PagePartitionerPool
{
    private final Supplier<PagePartitioner> pagePartitionerSupplier;
    /**
     * Maximum number of free {@link PagePartitioner}s.
     * In normal conditions, in the steady state,
     * the number of free {@link PagePartitioner}s is going to be close to 0.
     * There is a possible case though, where initially big number of concurrent drivers, say 128,
     * drops to a small number e.g., 32 in a steady state. This could cause a lot of memory
     * to be retained by the unused buffers.
     * To defend against that, {@link #maxFree} limits the number of free buffers,
     * thus limiting unused memory.
     */
    private final int maxFree;

    private final ReentrantLock thisLock = new ReentrantLock();

    @GuardedBy("thisLock")
    private final Queue<PagePartitioner> free = new ArrayDeque<>();
    @GuardedBy("thisLock")
    private boolean closed;

    public PagePartitionerPool(int maxFree, Supplier<PagePartitioner> pagePartitionerSupplier)
    {
        this.maxFree = maxFree;
        this.pagePartitionerSupplier = requireNonNull(pagePartitionerSupplier, "pagePartitionerSupplier is null");
    }

    public PagePartitioner poll()
    {
        try (CloseableLock<ReentrantLock> ignored = closeable(thisLock)) {
            checkArgument(!closed, "The pool is already closed");
            return free.isEmpty() ? pagePartitionerSupplier.get() : free.poll();
        }
    }

    public void release(PagePartitioner pagePartitioner)
    {
        // pagePartitioner.close can take a long time (flush->serialization), we want to keep it out of the synchronized block
        boolean shouldRetain;
        try (CloseableLock<ReentrantLock> ignored = closeable(thisLock)) {
            shouldRetain = !closed && free.size() < maxFree;
            if (shouldRetain) {
                free.add(pagePartitioner);
            }
        }
        if (!shouldRetain) {
            pagePartitioner.close();
        }
    }

    public void close()
    {
        // pagePartitioner.close can take a long time (flush->serialization), we want to keep it out of the synchronized block
        Collection<PagePartitioner> toClose = markClosed();
        closeSafely(toClose);
    }

    private static void closeSafely(Collection<PagePartitioner> toClose)
    {
        try (Closer closer = Closer.create()) {
            toClose.forEach(closer::register);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Collection<PagePartitioner> markClosed()
    {
        try (CloseableLock<ReentrantLock> ignored = closeable(thisLock)) {
            closed = true;
            List<PagePartitioner> toClose = ImmutableList.copyOf(free);
            free.clear();
            return toClose;
        }
    }
}
