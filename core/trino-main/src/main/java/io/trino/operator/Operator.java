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
import io.trino.execution.TaskId;
import io.trino.spi.Page;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public interface Operator
        extends AutoCloseable
{
    ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    OperatorContext getOperatorContext();

    /**
     * Returns a future that will be completed when the operator becomes
     * unblocked.  If the operator is not blocked, this method should return
     * {@code NOT_BLOCKED}.
     */
    default ListenableFuture<Void> isBlocked()
    {
        return NOT_BLOCKED;
    }

    /// When this operator is blocked (see [#isBlocked()]), the id of the pipeline whose output it is
    /// waiting on, if that producer is another pipeline in the same task. The scheduler donates
    /// priority to that pipeline while this operator is blocked so the dependency clears sooner. Empty
    /// when the block has no identifiable producer pipeline.
    default OptionalInt getBlockedProducerPipeline()
    {
        return OptionalInt.empty();
    }

    /// When this operator is blocked (see [#isBlocked()]), the ids of the producer tasks it is waiting
    /// on over an exchange (e.g. the upstream-stage tasks feeding an exchange source). The scheduler
    /// donates priority to any that are co-located on this worker so they drain sooner. Empty when the
    /// block has no identifiable producer tasks.
    default List<TaskId> getBlockedProducerTasks()
    {
        return List.of();
    }

    /**
     * Returns true if and only if this operator can accept an input page.
     */
    boolean needsInput();

    /**
     * Adds an input page to the operator.  This method will only be called if
     * {@code needsInput()} returns true.
     */
    void addInput(Page page);

    /**
     * Gets an output page from the operator.  If no output data is currently
     * available, return null.
     */
    Page getOutput();

    /**
     * After calling this method operator should revoke all reserved revocable memory.
     * As soon as memory is revoked returned future should be marked as done.
     * <p>
     * Spawned threads cannot modify OperatorContext because it's not thread safe.
     * For this purpose implement {@link #finishMemoryRevoke()}
     * <p>
     * Since memory revoking signal is delivered asynchronously to the Operator, implementation
     * must gracefully handle the case when there no longer is any revocable memory allocated.
     * <p>
     * After this method is called on Operator the Driver is disallowed to call most of
     * processing methods on it
     * ({@link #isBlocked()}/{@link #needsInput()}/{@link #addInput(Page)}/{@link #getOutput()})
     * until {@link #finishMemoryRevoke()} is called. {@link #finish()} is the only processing
     * method that can be called during that time and {@link #close()} remains callable
     * at any time.
     */
    default ListenableFuture<Void> startMemoryRevoke()
    {
        return NOT_BLOCKED;
    }

    /**
     * Clean up and release resources after completed memory revoking. Called by driver
     * once future returned by startMemoryRevoke is completed.
     */
    default void finishMemoryRevoke() {}

    /**
     * Notifies the operator that no more pages will be added and the
     * operator should finish processing and flush results. This method
     * will not be called if the Task is already failed or canceled.
     */
    void finish();

    /**
     * Is this operator completely finished processing and no more
     * output pages will be produced.
     */
    boolean isFinished();

    /**
     * This method will always be called before releasing the Operator reference.
     */
    @Override
    default void close()
            throws Exception
    {}
}
