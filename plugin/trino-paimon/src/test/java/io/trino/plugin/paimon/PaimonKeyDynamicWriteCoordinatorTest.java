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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonKeyDynamicWriteCoordinatorTest
{
    @Test
    void testSameTableWritesAreSerializedUntilQueryCleanup()
            throws Exception
    {
        PaimonKeyDynamicWriteCoordinator coordinator = new PaimonKeyDynamicWriteCoordinator();
        coordinator.acquire("first", "db.table");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<PaimonKeyDynamicWriteCoordinator.Lease> second =
                    executor.submit(() -> coordinator.acquire("second", "db.table"));
            Thread.sleep(100);
            assertThat(second).isNotDone();

            coordinator.releaseQuery("first");
            PaimonKeyDynamicWriteCoordinator.Lease secondLease = second.get(10, TimeUnit.SECONDS);
            assertThatCode(() -> coordinator.releaseQuery("second")).doesNotThrowAnyException();
            assertThat(secondLease).isNotNull();
        }
        finally {
            coordinator.releaseQuery("first");
            coordinator.releaseQuery("second");
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testQueryCleanupIsIdempotent()
    {
        PaimonKeyDynamicWriteCoordinator coordinator = new PaimonKeyDynamicWriteCoordinator();
        coordinator.acquire("query", "db.table");

        assertThatCode(() -> {
            coordinator.releaseQuery("query");
            coordinator.releaseQuery("query");
        }).doesNotThrowAnyException();
    }

    @Test
    void testAcquireTimesOutInsteadOfWaitingForever()
    {
        PaimonKeyDynamicWriteCoordinator coordinator =
                new PaimonKeyDynamicWriteCoordinator(Duration.ofMillis(50));
        coordinator.acquire("first", "db.table");

        assertThatThrownBy(() -> coordinator.acquire("second", "db.table"))
                .hasMessageContaining("Timed out")
                .hasMessageContaining("db.table");

        coordinator.releaseQuery("first");
    }

    @Test
    void testInterruptedAcquireDoesNotLeakOrDuplicatePermit()
            throws Exception
    {
        PaimonKeyDynamicWriteCoordinator coordinator = new PaimonKeyDynamicWriteCoordinator(Duration.ofMinutes(1));
        coordinator.acquire("first", "db.table");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> waiting = executor.submit(() -> coordinator.acquire("second", "db.table"));
            Thread.sleep(100);
            waiting.cancel(true);
            assertThatThrownBy(() -> waiting.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(CancellationException.class);

            coordinator.releaseQuery("first");
            PaimonKeyDynamicWriteCoordinator.Lease lease = coordinator.acquire("third", "db.table");
            assertThat(lease).isNotNull();
            coordinator.releaseQuery("third");
        }
        finally {
            coordinator.releaseQuery("first");
            coordinator.releaseQuery("second");
            coordinator.releaseQuery("third");
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
