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

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;

import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * <p>Semaphore-like structure that allows for tracking reservations to avoid double-reserving or double-releasing.</p>
 *
 * <p>Callers are expected to call {@link #reserve()} to acquire a slot, and then {@link #register(T)} to associate
 * an entity with the reservation.</p>
 *
 * <p>Upon completion, callers should call {@link #release(T)} to release the reservation.</p>
 */
@ThreadSafe
final class Reservation<T>
{
    private final Semaphore semaphore;
    private final Set<T> reservations = new HashSet<>();

    public Reservation(int slots)
    {
        semaphore = new Semaphore(slots);
    }

    public int availablePermits()
    {
        return semaphore.availablePermits();
    }

    public void reserve()
            throws InterruptedException
    {
        semaphore.acquire();
    }

    public synchronized void register(T entry)
    {
        checkArgument(!reservations.contains(entry), "Already acquired: %s", entry);
        reservations.add(entry);
    }

    public synchronized void release(T entry)
    {
        checkArgument(reservations.contains(entry), "Already released: %s", entry);
        reservations.remove(entry);

        semaphore.release();
    }

    public synchronized Set<T> reservations()
    {
        return ImmutableSet.copyOf(reservations);
    }

    @Override
    public synchronized String toString()
    {
        return new StringJoiner(", ", Reservation.class.getSimpleName() + "[", "]")
                .add("semaphore=" + semaphore)
                .add("reservations=" + reservations)
                .toString();
    }
}
