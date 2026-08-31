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
package io.trino.plugin.paimon;

import com.google.common.util.concurrent.Striped;
import io.trino.spi.TrinoException;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_METADATA_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Serializes KEY_DYNAMIC writes handled by one Trino coordinator.
 *
 * <p>The permit is held from begin-write bootstrap through commit. A semaphore
 * is used instead of a thread-affine lock because Trino may finish metadata
 * operations on a different coordinator thread from the one that planned the
 * write. The stripe count bounds coordinator memory while keeping unrelated
 * tables independent in normal operation.
 */
final class PaimonKeyDynamicWriteCoordinator
{
    private static final int STRIPE_COUNT = 1024;
    private static final Duration DEFAULT_ACQUIRE_TIMEOUT = Duration.ofMinutes(10);

    private final Striped<Semaphore> tableLocks = Striped.semaphore(STRIPE_COUNT, 1);
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Lease>> leasesByQuery = new ConcurrentHashMap<>();
    private final Duration acquireTimeout;

    PaimonKeyDynamicWriteCoordinator()
    {
        this(DEFAULT_ACQUIRE_TIMEOUT);
    }

    PaimonKeyDynamicWriteCoordinator(Duration acquireTimeout)
    {
        this.acquireTimeout = requireNonNull(acquireTimeout, "acquireTimeout is null");
        if (acquireTimeout.isZero() || acquireTimeout.isNegative()) {
            throw new IllegalArgumentException("acquireTimeout must be positive");
        }
    }

    Lease acquire(String queryId, String tableName)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(tableName, "tableName is null");
        Lease lease = new Lease(queryId, tableName, tableLocks.get(tableName));
        leasesByQuery.compute(queryId, (_, leases) -> {
            if (leases == null) {
                leases = new CopyOnWriteArrayList<>();
            }
            leases.add(lease);
            return leases;
        });
        try {
            lease.acquire();
            return lease;
        }
        catch (RuntimeException e) {
            remove(lease);
            throw e;
        }
    }

    void releaseQuery(String queryId)
    {
        requireNonNull(queryId, "queryId is null");
        CopyOnWriteArrayList<Lease> leases = leasesByQuery.remove(queryId);
        if (leases != null) {
            leases.forEach(Lease::release);
        }
    }

    private void remove(Lease lease)
    {
        CopyOnWriteArrayList<Lease> leases = leasesByQuery.get(lease.queryId());
        if (leases == null) {
            return;
        }
        leases.remove(lease);
        if (leases.isEmpty()) {
            leasesByQuery.remove(lease.queryId(), leases);
        }
    }

    final class Lease
    {
        private final String queryId;
        private final String tableName;
        private final Semaphore semaphore;
        private final AtomicBoolean released = new AtomicBoolean();
        private final AtomicBoolean permitHeld = new AtomicBoolean();

        private Lease(String queryId, String tableName, Semaphore semaphore)
        {
            this.queryId = queryId;
            this.tableName = tableName;
            this.semaphore = semaphore;
        }

        private void acquire()
        {
            try {
                if (!semaphore.tryAcquire(acquireTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    throw new TrinoException(
                            PAIMON_METADATA_ERROR,
                            "Timed out after " + acquireTimeout + " waiting for the Paimon KEY_DYNAMIC write slot for table "
                                    + tableName);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TrinoException(
                        PAIMON_METADATA_ERROR,
                        "Interrupted while waiting for the Paimon KEY_DYNAMIC write slot for table " + tableName,
                        e);
            }
            if (!permitHeld.compareAndSet(false, true)) {
                semaphore.release();
                throw new TrinoException(
                        PAIMON_METADATA_ERROR,
                        "Paimon KEY_DYNAMIC write acquired a duplicate table permit for " + tableName);
            }
            if (released.get() && permitHeld.compareAndSet(true, false)) {
                semaphore.release();
                throw new TrinoException(
                        PAIMON_METADATA_ERROR,
                        "Paimon KEY_DYNAMIC write was cancelled while waiting for table " + tableName);
            }
        }

        private void release()
        {
            if (released.compareAndSet(false, true)) {
                if (permitHeld.compareAndSet(true, false)) {
                    semaphore.release();
                }
                remove(this);
            }
        }

        private String queryId()
        {
            return queryId;
        }
    }
}
