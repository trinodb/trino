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
import io.trino.spi.Page;
import jakarta.annotation.Nullable;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public interface Operator
        extends AutoCloseable
{
    ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    OperatorContext getOperatorContext();

    /**
     * Whether this operator can emit {@link MaskedPage} output instead of materialized pages. The
     * driver reads masked output only when the downstream operator returns true from
     * {@link #supportsMaskedInput}; otherwise {@link #getOutput} is used.
     */
    default boolean producesMaskedOutput()
    {
        return false;
    }

    /**
     * Analog of {@link #getOutput} producing a {@link MaskedPage}. The returned page is valid only
     * until the next output request on this operator.
     */
    @Nullable
    default MaskedPage getMaskedOutput()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Whether this operator can consume {@link MaskedPage} input instead of materialized pages.
     */
    default boolean supportsMaskedInput()
    {
        return false;
    }

    /**
     * Analog of {@link #addInput} accepting a {@link MaskedPage}. The page is valid only for the
     * duration of this call; the consumer must decode or copy everything it needs before returning.
     */
    default void addMaskedInput(MaskedPage maskedPage)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a future that will be completed when the operator becomes
     * unblocked.  If the operator is not blocked, this method should return
     * {@code NOT_BLOCKED}.
     */
    default ListenableFuture<Void> isBlocked()
    {
        return NOT_BLOCKED;
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
