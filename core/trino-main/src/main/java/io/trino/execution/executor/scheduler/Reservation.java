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
package io.trino.execution.executor.scheduler;

import com.google.errorprone.annotations.ThreadSafe;

import java.util.StringJoiner;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * <p>Semaphore-like structure that allows for tracking reservations to avoid double-reserving or double-releasing.</p>
 *
 * <p>Callers are expected to call {@link #reserve()} or {@link #tryReserve()} to acquire a slot,
 * and then {@link #register(T)} to associate an entity with the reservation.</p>
 *
 * <p>Upon completion, callers should call {@link #release(T)} to release the reservation.</p>
 *
 * <p>Ownership is recorded on the entity itself rather than in a set held by this class, so that
 * registering and releasing stay off any shared lock. They sit on the path of every block, yield
 * and task start.</p>
 */
@ThreadSafe
final class Reservation<T extends Reservable>
{
    private final Semaphore semaphore;
    private final int slots;

    public Reservation(int slots)
    {
        this.slots = slots;
        semaphore = new Semaphore(this.slots);
    }

    public int totalSlots()
    {
        return slots;
    }

    public int availableSlots()
    {
        return semaphore.availablePermits();
    }

    public void reserve()
            throws InterruptedException
    {
        semaphore.acquire();
    }

    /**
     * Non-blocking variant of {@link #reserve()}.
     *
     * @return true if a slot was acquired, in which case the caller must eventually either
     *         {@link #register(T)} and {@link #release(T)} it, or hand it back via
     *         {@link #releaseUnregistered()}
     */
    public boolean tryReserve()
    {
        return semaphore.tryAcquire();
    }

    /**
     * Hands back a slot acquired via {@link #tryReserve()} that was never associated with an
     * entity through {@link #register(T)}.
     */
    public void releaseUnregistered()
    {
        semaphore.release();
    }

    public void register(T entry)
    {
        checkArgument(entry.tryMarkReserved(), "Already acquired: %s", entry);
    }

    public void release(T entry)
    {
        checkArgument(entry.tryMarkReleased(), "Already released: %s", entry);

        semaphore.release();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", Reservation.class.getSimpleName() + "[", "]")
                .add("semaphore=" + semaphore)
                .toString();
    }
}
