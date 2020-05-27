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
package io.prestosql.plugin.base.splitloader;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.MoreFutures;
import io.prestosql.plugin.base.util.ResumableTask;
import io.prestosql.plugin.base.util.ResumableTasks;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public abstract class AbstractAsyncSplitLoader<S>
        implements AsyncSplitLoader<S>
{
    protected static final ListenableFuture<?> COMPLETED_FUTURE = immediateFuture(null);

    // Purpose of this lock:
    // * Write lock: when you need a consistent view across partitions, fileIterators, and hiveSplitSource.
    // * Read lock: when you need to modify any of the above.
    //   Make sure the lock is held throughout the period during which they may not be consistent with each other.
    // Details:
    // * When write lock is acquired, except the holder, no one can do any of the following:
    // ** poll from (or check empty) partitions
    // ** poll from (or check empty) or push to fileIterators
    // ** push to hiveSplitSource
    // * When any of the above three operations is carried out, either a read lock or a write lock must be held.
    // * When a series of operations involving two or more of the above three operations are carried out, the lock
    //   must be continuously held throughout the series of operations.
    // Implications:
    // * if you hold a read lock but not a write lock, you can do any of the above three operations, but you may
    //   see a series of operations involving two or more of the operations carried out half way.
    private final ReadWriteLock taskExecutionLock = new ReentrantReadWriteLock();
    private AbstractAsyncSplitSource<S> splitSource;

    private final Executor executor;
    private final int loaderConcurrency;
    private volatile boolean stopped;

    protected AbstractAsyncSplitLoader(int loaderConcurrency, Executor executor)
    {
        this.loaderConcurrency = loaderConcurrency;
        this.executor = executor;
        this.stopped = false;
    }

    protected abstract ListenableFuture<?> loadSplits() throws IOException;

    protected abstract boolean isFinished();

    private void invokeNoMoreSplitsIfNecessary()
    {
        taskExecutionLock.readLock().lock();
        try {
            // This is an opportunistic check to avoid getting the write lock unnecessarily
            if (!isFinished()) {
                return;
            }
        }
        catch (Exception e) {
            fail(e);
            checkState(isStopped(), "Task is not marked as stopped even though it failed");
            return;
        }
        finally {
            taskExecutionLock.readLock().unlock();
        }

        taskExecutionLock.writeLock().lock();
        try {
            // the write lock guarantees that no one is operating on the partitions, fileIterators, or hiveSplitSource, or half way through doing so.
            if (isFinished()) {
                // It is legal to call `noMoreSplits` multiple times or after `stop` was called.
                // Nothing bad will happen if `noMoreSplits` implementation calls methods that will try to obtain a read lock because the lock is re-entrant.
                noMoreSplits();
            }
        }
        catch (Exception e) {
            fail(e);
            checkState(isStopped(), "Task is not marked as stopped even though it failed");
        }
        finally {
            taskExecutionLock.writeLock().unlock();
        }
    }

    @Override
    public void start(AbstractAsyncSplitSource<S> splitSource)
    {
        this.splitSource = splitSource;
        for (int i = 0; i < loaderConcurrency; i++) {
            ListenableFuture<?> future = ResumableTasks.submit(executor, new SplitLoaderTask());
            MoreFutures.addExceptionCallback(future, this.splitSource::fail); // best effort; hiveSplitSource could be already completed
        }
    }

    @Override
    public void stop()
    {
        stopped = true;
    }

    protected ListenableFuture<?> addToQueue(List<S> splits)
    {
        ListenableFuture<?> lastResult = immediateFuture(null);
        for (S split : splits) {
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    protected ListenableFuture<?> addToQueue(S split)
    {
        return splitSource.addToQueue(split);
    }

    protected void fail(Throwable e)
    {
        splitSource.fail(e);
    }

    protected void noMoreSplits()
    {
        splitSource.noMoreSplits();
    }

    protected boolean isStopped()
    {
        return stopped;
    }

    private class SplitLoaderTask
            implements ResumableTask
    {
        @Override
        public TaskStatus process()
        {
            while (true) {
                if (stopped) {
                    return TaskStatus.finished();
                }
                ListenableFuture<?> future;
                taskExecutionLock.readLock().lock();
                try {
                    future = loadSplits();
                }
                catch (Throwable e) {
                    // TODO: Make these error codes non-hive specific
//                    if (e instanceof IOException) {
//                        e = new PrestoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR, e);
//                    }
//                    else if (!(e instanceof PrestoException)) {
//                        e = new PrestoException(UNKNOWN_ER, e);
//                    }
                    // Fail the split source before releasing the execution lock
                    // Otherwise, a race could occur where the split source is completed before we fail it.
                    splitSource.fail(e);
                    checkState(stopped);
                    return TaskStatus.finished();
                }
                finally {
                    taskExecutionLock.readLock().unlock();
                }
                invokeNoMoreSplitsIfNecessary();
                if (!future.isDone()) {
                    return TaskStatus.continueOn(future);
                }
            }
        }
    }
}
