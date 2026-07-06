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
package io.trino.operator;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.ExchangedPage;
import io.trino.spi.Page;

import java.io.Closeable;
import java.util.List;

public interface DirectExchangeBuffer
        extends Closeable
{
    /**
     * This method may be called by multiple independent client concurrently.
     * Implementations must ensure the cancellation of a future by one of the clients
     * doesn't cancel futures obtained by other clients.
     */
    ListenableFuture<Void> isBlocked();

    ExchangedPage pollPage();

    void addTask(TaskId taskId);

    void addPages(TaskId taskId, List<Slice> pages);

    /**
     * Returns whether this buffer accepts pages passed by reference from a task
     * running on this node. Raw pages cannot be spooled, so buffers used with
     * fault tolerant execution do not support them.
     */
    default boolean supportsRawPages()
    {
        return false;
    }

    /**
     * Adds raw pages received by reference from a task running on this node.
     */
    default void addRawPages(TaskId taskId, List<Page> pages)
    {
        throw new UnsupportedOperationException("raw pages are not supported by " + getClass().getSimpleName());
    }

    void taskFinished(TaskId taskId);

    void taskFailed(TaskId taskId, Throwable t);

    void noMoreTasks();

    boolean isFinished();

    boolean isFailed();

    long getRemainingCapacityInBytes();

    long getRetainedSizeInBytes();

    long getMaxRetainedSizeInBytes();

    int getBufferedPageCount();

    long getSpilledBytes();

    int getSpilledPageCount();

    @Override
    void close();
}
